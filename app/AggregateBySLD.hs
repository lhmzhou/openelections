{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFunctor      #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections      #-}
{-# LANGUAGE TypeApplications   #-}
module Main where

import qualified Data.Csv                      as C
import           Data.Csv                       ( (.:) )
import qualified Data.List                     as L
import qualified Data.Map                      as M
import           Data.Maybe                     ( isJust
                                                , fromJust
                                                , fromMaybe
                                                )
import           Data.Ord                       ( Ordering )
import qualified Data.Set                      as S
import qualified Data.Text                     as T
import qualified Data.Text.IO                  as T
import qualified Data.Char                     as TC
import qualified Data.ByteString.Lazy          as BL
import qualified Data.Vector                   as V
import           Text.Read                      ( readMaybe )

import           Control.Applicative            ( (<|>) )
import           Control.Monad                  ( when
                                                , forM
                                                )
import qualified Control.Exception             as E
import qualified Control.Foldl                 as FL
import qualified Control.MapReduce             as MR

import           GHC.Generics                   ( Generic )
import           Data.Typeable                  ( Typeable )
import qualified System.Directory              as SD

data MyException = MiscException T.Text deriving (Show, Typeable)

instance E.Exception MyException

data CandidateVote =
  CandidateVote { candOffice :: !T.Text
                , candName :: !T.Text
                , candParty :: !T.Text
                , candVotes :: !Int
                } deriving (Generic, Show)

scaleVotes :: Double -> CandidateVote -> CandidateVote
scaleVotes x (CandidateVote o n p v) =
  CandidateVote o n p (round $ x * realToFrac v)

data PrecinctVote =
  PrecinctVote { county :: !T.Text
               , precinct :: !T.Text
               , districtM :: !(Maybe Int)
               , pVote :: CandidateVote
               } deriving (Generic, Show)

type PKey = (T.Text, T.Text) -- (county, precinct)

pKey :: PrecinctVote -> PKey
pKey pv = (county pv, precinct pv)

instance C.FromNamedRecord PrecinctVote where
  parseNamedRecord m = PrecinctVote
                       <$> m .: "county"
                       <*> m .: "precinct"
                       <*> (fmap (readMaybe @Int . L.filter TC.isDigit) $ m .: "district")
                       <*> (CandidateVote
                            <$> m .: "office"
                            <*> m .: "party"
                            <*> m .: "candidate"
                            <*> (fmap (fromMaybe 0 . readMaybe @Int) $ m .: "votes")
                           )

fixOffice :: PrecinctVote -> PrecinctVote
fixOffice pv@(PrecinctVote c p dM (CandidateVote o n pa v)) = case dM of
  Nothing -> pv
  Just d  -> PrecinctVote
    c
    p
    dM
    (CandidateVote (o <> ": district " <> (T.pack $ show d)) n pa v)

data StateConfig = StateConfig { getFilePaths :: IO ([FilePath], [FilePath]) }

gaConfig = StateConfig
  (do
    let dir = "openelections-data-ga/2018/"
        addDir f = dir ++ f
    allFiles <- SD.listDirectory dir
    let elexFiles = fmap addDir
          $ L.filter (L.isPrefixOf "20181106__ga__general__") allFiles
        backupFiles = fmap addDir
          $ L.filter (L.isPrefixOf "20180522__ga__primary__") allFiles
    return (elexFiles, backupFiles)
  )


txConfig = StateConfig
  (do
    let dir = "openelections-data-tx/2018/counties/"
        addDir f = dir ++ f
    allFiles <- SD.listDirectory dir
    let elexFiles = fmap addDir
          $ L.filter (L.isPrefixOf "20181106__tx__general__") allFiles
        backupFiles = fmap addDir
          $ L.filter (L.isPrefixOf "20180306__tx__primary__") allFiles
    return (elexFiles, backupFiles)
  )

main :: IO ()
main = do
  let stateConfig = txConfig
  (elexFiles, backupPrecFiles) <- getFilePaths stateConfig
  parsedElection'              <- parseFiles elexFiles
  let parsedElection = fmap (\(a, b) -> (a, fixOffice b)) parsedElection'
  parsedBackup <- parseFiles backupPrecFiles
  putStrLn $ "constructing backup dictionary of precinct -> SLD mappings"
  backupSLDsBP <- case FL.foldM sldsByPrecinctFM parsedBackup of
    Left err -> E.throw $ MiscException
      (  "Backup precinct -> SLDs dictionary build failure: "
      <> (T.pack $ show err)
      )
    Right x -> return x
  putStrLn
    $ "Doing pass 1: sldsByPrecinct, vboPrecinct and primary precinct -> SLDs dictionary"
  let allFM =
        (,,)
          <$> (FL.generalize precinctsBySLDF)
          <*> (FL.generalize tvboFromPrecinctsF)
          <*> sldsByPrecinctFM
  (precinctsBySLDs, vboPrecinct, sldsByPrecinct) <-
    case FL.foldM allFM parsedElection of
      Left err ->
        E.throw $ MiscException ("Pass 1 Failure: " <> (T.pack $ show err))
      Right x -> return x
  putStrLn $ "pass 1 complete.\npass 2: votesBySLD and vboHouse, vboSenate"
  putStrLn $ "Backfilling primary dictionary:"
  updatedSLDsByPrecinct <- backfillDictionary sldsByPrecinct backupSLDsBP
  votesBySLD            <-
    case
      FL.foldM (votesBySLDFM updatedSLDsByPrecinct) (fmap snd parsedElection)
    of
      Left err -> E.throw
        $ MiscException ("Vote re-allocation error: " <> (T.pack $ show err))
      Right x -> return x
  let votesF =
        (,)
          <$> FL.prefilter (isHouse . fst) tvboFromSLDF
          <*> FL.prefilter (isSenate . fst) tvboFromSLDF
      (vboHouse, vboSenate) = FL.fold votesF votesBySLD
      voteComp x y = if x == y then Nothing else Just (x - y)
      pVsHouse      = M.differenceWith voteComp vboPrecinct vboHouse
      pVsSenate     = M.differenceWith voteComp vboPrecinct vboSenate
      houseVsSenate = M.differenceWith voteComp vboHouse vboSenate
  putStrLn $ "Precinct vs House:\n" ++ show pVsHouse
  putStrLn $ "Precinct vs Senate:\n" ++ show pVsSenate
  putStrLn $ "House vs Senate:\n" ++ show houseVsSenate
  let output = vbSLDHeader <> "\n" <> T.intercalate
        "\n"
        (fmap vbSLDtoCSV $ L.sortBy outputCompare $ L.filter outputFilter
                                                             votesBySLD
        )
  T.writeFile "VotesByStateLegistiveDistrict.csv" output
  return ()

outputFilter :: (SLD (), CandidateVote) -> Bool
outputFilter (_, (CandidateVote o _ _ _)) =
  let hasWord word text = not . T.null . snd $ word `T.breakOn` text
      isGov    = hasWord "Governor" o
      isSenate = hasWord "Senator" o && not (hasWord "State" o)
      isHouse  = hasWord "Representative" o && not (hasWord "State" o)
  in  isGov || isSenate || isHouse

outputCompare :: (SLD (), CandidateVote) -> (SLD (), CandidateVote) -> Ordering
outputCompare (sldA, (CandidateVote oA nA _ _)) (sldB, (CandidateVote oB nB _ _))
  = compare oA oB <> compare sldA sldB <> compare nA nB


backfillDictionary
  :: SLDsByPrecinct Double
  -> SLDsByPrecinct Double
  -> IO (SLDsByPrecinct Double)
backfillDictionary electionD backupD = do
  let
    missingHouse  = L.null . L.filter isHouse
    missingSenate = L.null . L.filter isSenate
    multiHouse    = (> 1) . L.length . L.filter isHouse
    multiSenate   = (> 1) . L.length . L.filter isSenate
    addFromBackup isType (p, slds) =
      case fmap (L.filter isType) (M.lookup p backupD) of
        Nothing ->
          E.throw
            $  MiscException
            $  "Failed to find precinct=\""
            <> (T.pack $ show p)
            <> "\" in backup dictionary."
        Just [] ->
          E.throw
            $  MiscException
            $  "Backup entry for precinct=\""
            <> (T.pack $ show p)
            <> "\" is also empty."
        Just newSLDs -> return (p, slds ++ newSLDs)

    findF =
      (,,,)
        <$> FL.prefilter (missingHouse . snd) FL.list
        <*> FL.prefilter (missingSenate . snd) FL.list
        <*> FL.prefilter (multiHouse . snd) FL.list
        <*> FL.prefilter (multiSenate . snd) FL.list
    (needBackupH, needBackupS, multiH, multiS) =
      FL.fold findF (M.toList electionD)
  withBackupH <- traverse (addFromBackup isHouse) needBackupH
  withBackupS <- traverse (addFromBackup isSenate) needBackupS
  putStrLn $ "House from backup: " ++ show withBackupH
  putStrLn $ "Senate from backup: " ++ show withBackupS
  putStrLn $ "Multi-House: " ++ show multiH
  putStrLn $ "Multi-Senate: " ++ show multiS
  let fromBackupM =
        M.unionWith (<>) (M.fromList withBackupH) (M.fromList withBackupS) -- in case same precinct is on both lists
      newSLDsByPrecinct = M.union fromBackupM electionD -- prefer updated  
  return newSLDsByPrecinct



parseFiles :: [FilePath] -> IO (V.Vector (Maybe (SLD Int), PrecinctVote))
parseFiles fps = do
  pVotes <-
    mconcat
      <$> (forM fps $ \fp -> do
            putStrLn $ "loading/parsing \"" ++ fp ++ "\""
            precinctCSV <- BL.readFile fp
            case C.decodeByName @PrecinctVote precinctCSV of
              Left err -> E.throw
                $ MiscException ("CSV Parse Failure: " <> (T.pack $ show err))
              Right (_, v) -> return v
          )
  slds <- case (sequence $ fmap parseSLDFromPV pVotes) of
    Left err ->
      E.throw $ MiscException ("SLD Parse Failure: " <> (T.pack $ show err))
    Right x -> return x
  return $ V.zip slds pVotes

data Candidate = Candidate { cName :: T.Text, cParty :: T.Text }

type VotesByCandidate = M.Map T.Text Int -- votes by candidate

type VotesByOffice = M.Map T.Text VotesByCandidate

type VotesByPrecinct = M.Map T.Text VotesByOffice

data SLD a = House Int a | Senate Int a deriving (Show, Eq, Ord, Functor)

isHouse (House _ _) = True
isHouse _           = False

isSenate (Senate _ _) = True
isSenate _            = False

type PrecinctsBySLD = M.Map (SLD ()) (S.Set PKey)
type SLDsByPrecinct a = M.Map PKey [SLD a]

data ProcessedPrecincts = ProcessedPrecincts { precinctsBySLD :: PrecinctsBySLD
                                             , votesByPrecinct :: VotesByPrecinct
                                             } deriving (Show)

votesBySLDFM
  :: SLDsByPrecinct Double
  -> FL.FoldM (Either T.Text) PrecinctVote [(SLD (), CandidateVote)]
votesBySLDFM sldsByP = MR.mapReduceFoldM
  (MR.UnpackM $ precinctVotesToSLDVotes sldsByP)
  (MR.generalizeAssign $ MR.Assign
    (\(sld, pvote) ->
      ( (sld, candOffice pvote, candName pvote, candParty pvote)
      , candVotes pvote
      )
    )
  )
  (MR.generalizeReduce $ MR.foldAndLabel
    FL.sum
    (\(sld, co, cn, cp) v -> (sld, CandidateVote co cn cp v))
  )

vbSLDHeader :: T.Text =
  "State Legislative District,Office,Candidate,Party,Votes"

vbSLDtoCSV :: (SLD (), CandidateVote) -> T.Text
vbSLDtoCSV (sld, (CandidateVote o n p v)) =
  sldName sld <> "," <> o <> "," <> n <> "," <> p <> "," <> (T.pack $ show v)

sldName :: SLD a -> T.Text
sldName (House  n _) = "House " <> (T.pack $ show n)
sldName (Senate n _) = "Senate " <> (T.pack $ show n)

sldLabel :: SLD a -> SLD ()
sldLabel = fmap (const ())

sldData :: SLD a -> a
sldData (House  _ x) = x
sldData (Senate _ x) = x

sumDistrictData :: Num a => [SLD a] -> [SLD a]
sumDistrictData slds =
  fmap (\(l, d) -> fmap (const d) l) $ M.toList $ M.fromListWith (+) $ zip
    (fmap sldLabel slds)
    (fmap sldData slds)

precinctVotesToSLDVotes
  :: SLDsByPrecinct Double
  -> PrecinctVote
  -> Either T.Text [(SLD (), CandidateVote)]
precinctVotesToSLDVotes sldsByP pv = case M.lookup (pKey pv) sldsByP of
  Nothing   -> Left $ "No SLDs found for precinct=\"" <> (precinct pv) <> "\"."
  Just slds -> do
    let withSLD pv sld = (sldLabel sld, scaleVotes (sldData sld) $ pVote pv)
    return $ fmap (withSLD pv) slds

precinctsBySLDF :: FL.Fold (Maybe (SLD Int), PrecinctVote) PrecinctsBySLD
precinctsBySLDF = fmap (fmap S.fromList . M.fromList) $ MR.mapReduceFold
  (MR.filterUnpack (isJust . fst))
  (MR.assign (fromJust . fst) (pKey . snd))
  (MR.foldAndLabel FL.list (\sld ps -> (sldLabel sld, ps)))

sldsByPrecinctFM
  :: FL.FoldM
       (Either T.Text)
       (Maybe (SLD Int), PrecinctVote)
       (SLDsByPrecinct Double)
sldsByPrecinctFM = fmap M.fromList $ MR.mapReduceFoldM
  (MR.generalizeUnpack $ MR.filterUnpack (isJust . fst))
  (MR.generalizeAssign $ MR.assign (pKey . snd) (fromJust . fst))
  (MR.ReduceFoldM f)

f :: PKey -> FL.FoldM (Either T.Text) (SLD Int) (PKey, [SLD Double])
f p = fmap (p, ) $ MR.postMapM (sldsVotesToWeights p) (FL.generalize FL.list)

sldsVotesToWeights :: PKey -> [SLD Int] -> Either T.Text [SLD Double]
sldsVotesToWeights p slds = do
  let wgtError seatType p =
        (T.pack $ show p)
          <> " has >1 "
          <> seatType
          <> " districts but weights sum to 0: "
      toWeight total n = (realToFrac n) / (realToFrac total)
      weightSLDs p seatType slds = case sumDistrictData slds of
        []  -> Right []
        [x] -> Right [fmap (const 1) x]
        xs  -> do
          let t = FL.fold (FL.premap sldData FL.sum) xs
          when (t == 0) $ Left $ wgtError seatType p <> (T.pack $ show xs)
          return $ fmap (fmap $ toWeight t) xs
  whSLDs <- weightSLDs p "House" $ filter isHouse slds
  wsSLDs <- weightSLDs p "Senate" $ filter isSenate slds
  return $ whSLDs ++ wsSLDs


totalVotesByOfficeF :: FL.Fold (T.Text, Int) (M.Map T.Text Int)
totalVotesByOfficeF = fmap M.fromList $ MR.mapReduceFold
  MR.noUnpack
  (MR.assign fst snd)
  (MR.foldAndLabel FL.sum (\office votes -> (office, votes)))

tvboFromPrecinctsF = FL.premap
  (\(_, pv) -> (candOffice $ pVote pv, candVotes $ pVote pv))
  totalVotesByOfficeF

tvboFromSLDF =
  FL.premap (\(_, cv) -> (candOffice cv, candVotes cv)) totalVotesByOfficeF

hasWord x y = not . T.null . snd $ T.breakOn y x

distAtEnd :: T.Text -> Maybe Int
distAtEnd = readMaybe . T.unpack . T.takeWhileEnd TC.isDigit . T.strip

parseSLDFromPV pv =
  parseSLD (candOffice $ pVote pv) (districtM pv) (candVotes $ pVote pv)

parseSLD :: T.Text -> Maybe Int -> Int -> Either T.Text (Maybe (SLD Int))
parseSLD office districtM votes = do
  let officeHas = hasWord (T.toUpper office)
      isState   = officeHas "STATE"
      isHouse   = officeHas "REPRESENTATIVE"
      isSenate  = officeHas "SENATOR"
      isSLD     = isState && (isHouse || isSenate)
  case isSLD of
    False -> return Nothing
    True  -> do
      let dM = districtM <|> (distAtEnd office) -- uses first if possible
      sldFromOffice <- case (isHouse, isSenate) of
        (False, False) -> Left $ "Shouldn't be possible!"
        (True , True ) -> Left $ "Bad parse: office=\"" <> office <> "\""
        (True , False) -> return (\d v -> Just $ House d v)
        (False, True ) -> return (\d v -> Just $ Senate d v)
      case dM of
        Nothing ->
          Left
            $  "Bad district parse (district col was empty): office=\""
            <> office
            <> "\""
        Just d -> Right $ sldFromOffice d votes



--thinkOne :: V.Vector PrecinctVote -> ProcessedPrecincts 

{-
data PrecinctSLDs = UnFound | JustSenate Int | JustHouse Int | HouseSenate Int Int

addSLD p NonSLD _ = Left $ "NonSLD precinct should have been filtered out!"
addSLD _ (House x) UnFound        = Right $ JustHouse x
addSLD _ (House x) (JustSenate y) = Right $ HouseSenate x y
addSLD p (House x) (JustHouse  y) = if (x == y)
  then Right (JustHouse y)
  else
    Left
    $  "Error filling out SLDs for precinct=\""
    <> p
    <> "\".  Already had house="
    <> (T.pack $ show y)
    <> " and now found house="
    <> (T.pack $ show x)
addSLD p (House x) hs@(HouseSenate y _) = if (x == y)
  then Right hs
  else
    Left
    $  "Error filling out SLDs for precinct=\""
    <> p
    <> "\". Already had house="
    <> (T.pack $ show y)
    <> " and now found house="
    <> (T.pack $ show x)

addSLD _ (Senate x) UnFound        = Right $ JustSenate x
addSLD _ (Senate x) (JustHouse  y) = Right $ HouseSenate y x
addSLD p (Senate x) (JustSenate y) = if x == y
  then Right (JustSenate y)
  else
    Left
    $  "Error filling out SLDs for precinct=\""
    <> p
    <> "\". Already had senate="
    <> (T.pack $ show y)
    <> " and now found senate="
    <> (T.pack $ show x)
addSLD p (Senate x) hs@(HouseSenate _ y) = if x == y
  then Right hs
  else
    Left
    $  "Error filling out SLDs for precinct=\""
    <> p
    <> "\". Already had senate="
    <> (T.pack $ show y)
    <> " and now found senate="
    <> (T.pack $ show x)

sldsFromPrecinct :: SLDsByPrecinct -> T.Text -> Either T.Text [SLD]
sldsFromPrecinct sbp p = case M.lookup p sbp of
  Nothing    -> Left $ "p=\"" <> p <> "\" missing in SLD by precinct map"
  Just pslds -> case pslds of
    UnFound -> Left $ "No SLDs found for precint=\"" <> p <> "\"."
    JustHouse _ ->
      Left $ "No Senate district found for precint=\"" <> p <> "\"."
    JustSenate _ ->
      Left $ "No House district found for precint=\"" <> p <> "\"."
    HouseSenate hd sd -> Right $ [House hd, Senate sd]

psldF :: T.Text -> FL.Fold SLD (T.Text, PrecinctSLDs)
psldF p = FL.FoldM (\pslds sld -> addSLD p sld pslds)
                   (Right UnFound)
                   (\pslds -> return (p, pslds))

-}
