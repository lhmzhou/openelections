{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFunctor      #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections      #-}
{-# LANGUAGE TypeApplications   #-}
module Main where

import qualified Data.Csv                      as C
import qualified Data.Csv.Streaming            as CS
import           Data.Csv                       ( (.:) )
import qualified Data.List                     as L
import qualified Data.Map                      as M
import           Data.Maybe                     ( isJust
                                                , fromJust
                                                , fromMaybe
                                                )
import qualified Data.Bifunctor                as BF
import           Data.Either                    ( isRight )
import           Data.Ord                       ( Ordering )
import           Data.Monoid                    ( Any(..) )
import qualified Data.Set                      as Set
import qualified Data.Text                     as T
import qualified Data.Text.IO                  as T
import qualified Data.Text.Encoding            as T
import qualified Data.Char                     as TC
import qualified Data.ByteString               as B
import qualified Data.ByteString.Lazy          as BL

import qualified Data.Vector                   as V
import           Data.Word                      ( Word8 )
import           Text.Read                      ( readMaybe )

import           Control.Applicative            ( (<|>) )
import           Control.Monad                  ( when
                                                , join
                                                , forM
                                                )
import           Control.Monad.IO.Class         ( MonadIO(liftIO) )
import qualified Control.Exception             as E
import qualified Control.Foldl                 as FL
import qualified Control.MapReduce             as MR

import           GHC.Generics                   ( Generic )
import           Data.Typeable                  ( Typeable )
import qualified System.Directory              as SD
import qualified System.IO                     as SI

import qualified Streamly.Prelude              as S
import qualified Streamly                      as S
import qualified Streamly.Internal.FileSystem.File
                                               as SF
import qualified Streamly.Data.Fold            as SFL

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
               , pVote :: !CandidateVote
               } deriving (Generic, Show)

data PKey = PKey { pkCounty :: !T.Text, pkPrecinct :: !T.Text } deriving (Show, Eq, Ord)

pKey :: PrecinctVote -> PKey
pKey pv = PKey (county pv) (precinct pv)

instance C.FromNamedRecord PrecinctVote where
  parseNamedRecord m = PrecinctVote
                       <$> m .: "county"
                       <*> (fmap (T.filter (/= ' ') . T.toUpper) $ m .: "precinct")
                       <*> (fmap (readMaybe @Int . L.filter TC.isDigit) $ m .: "district")
                       <*> (CandidateVote
                            <$> m .: "office"
                            <*> m .: "party"
                            <*> m .: "candidate"
                            <*> (fmap (fromMaybe 0 . fmap round . readMaybe @Double) $ m .: "votes")
                           )

fixOffice :: PrecinctVote -> PrecinctVote
fixOffice pv@(PrecinctVote c p dM (CandidateVote o n pa v)) = case dM of
  Nothing -> pv
  Just d  -> PrecinctVote
    c
    p
    dM
    (CandidateVote (o <> ": district " <> (T.pack $ show d)) n pa v)

data StateConfig = StateConfig { getFilePaths :: IO ([FilePath], [FilePath])
                               , bySLDFile :: FilePath
                               , logFileM :: Maybe FilePath
                               }

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
  "results/GA_VotesByStateLegislativeDistrict.csv"
  (Just "logs/GA.log")


txConfig
  = let
      getFiles = do
        let
          dir = "openelections-data-tx/2018/counties/"
          addDir f = dir ++ f
          elexFiles =
            ["openelections-data-tx/2018/20181106__tx__general__precinct.csv"]
        allFiles <- SD.listDirectory dir
        let
          backupFiles =
            ["openelections-data-tx/2016/20161108__tx__general__precinct.csv"]
              ++ (fmap addDir $ L.filter
                   (L.isPrefixOf "20180306__tx__primary__")
                   allFiles
                 )
        return (elexFiles, backupFiles)
    in  StateConfig getFiles
                    "results/TX_VotesByStateLegislativeDistrict.csv"
                    (Just "logs/TX.log")

iaConfig = StateConfig
  (do
    let elexFiles =
          ["openelections-data-ia/2018/20181106__ia__general__precinct.csv"]
        backupFiles =
          [ "openelections-data-ia/2018/20180605__ia__primary__precinct.csv"
          , "openelections-data-ia/2016/20161108__ia__general__precinct.csv"
          , "openelections-data-ia/2014/20141104__ia__general__precinct.csv"
          ]
    return (elexFiles, backupFiles)
  )
  "results/IA_VotesByStateLegislativeDistrict.csv"
  (Just "logs/IA.log")

main :: IO ()
main = do
  let stateConfig = txConfig
      log         = maybe (T.hPutStrLn SI.stderr)
                          (\fp msg -> T.appendFile fp (msg <> "\n"))
                          (logFileM stateConfig)
      removeIfExists fp = do
        b <- SD.doesFileExist fp
        when b $ SD.removeFile fp
        return ()
  maybe (return ()) removeIfExists (logFileM stateConfig)
  (elexFiles, backupPrecFiles) <- getFilePaths stateConfig
  parsedElection'              <- parseFiles log elexFiles
  let parsedElection = fmap (\(a, b) -> (a, fixOffice b)) parsedElection'
  parsedBackup <- parseFiles log backupPrecFiles
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
  updatedSLDsByPrecinct <- backfillDictionary log sldsByPrecinct backupSLDsBP
  let votesBySLD =
        FL.fold (votesBySLDF updatedSLDsByPrecinct) (fmap snd parsedElection)
      votesF =
        (,)
          <$> FL.prefilter (isHouse . fst) tvboFromSLDF
          <*> FL.prefilter (isSenate . fst) tvboFromSLDF
      (vboHouse, vboSenate) = FL.fold votesF votesBySLD
      voteComp x y = if x == y then Nothing else Just (x - y)
      pVsHouse      = M.differenceWith voteComp vboPrecinct vboHouse
      pVsSenate     = M.differenceWith voteComp vboPrecinct vboSenate
      houseVsSenate = M.differenceWith voteComp vboHouse vboSenate
      prettyList    = L.intercalate "\n" . fmap show . M.toList
  log $ T.pack $ "Precinct vs House:\n" ++ prettyList pVsHouse
  log $ T.pack $ "Precinct vs Senate:\n" ++ prettyList pVsSenate
  log $ T.pack $ "House vs Senate:\n" ++ prettyList houseVsSenate
  let output = vbSLDHeader <> "\n" <> T.intercalate
        "\n"
        (fmap vbSLDtoCSV $ L.sortBy outputCompare $ L.filter outputFilter
                                                             votesBySLD
        )
  T.writeFile (bySLDFile stateConfig) output
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
  :: (T.Text -> IO ())
  -> SLDsByPrecinct Double
  -> SLDsByPrecinct Double
  -> IO (SLDsByPrecinct Double)
backfillDictionary log electionD backupD = do
  let
    missingHouse  = L.null . L.filter isHouse
    missingSenate = L.null . L.filter isSenate
    multiHouse    = (> 1) . L.length . L.filter isHouse
    multiSenate   = (> 1) . L.length . L.filter isSenate
    missingMsg slds = case (missingHouse slds, missingSenate slds) of
      (True, True) -> "missing all SLDs"
      (True, False) ->
        "missing state house district. Has " <> (T.pack $ show slds)
      (False, True) ->
        "missing state senate district. Has " <> (T.pack $ show slds)
    addFromBackup isType (p, slds) = do
      case fmap (L.filter isType) (M.lookup p backupD) of
        Nothing -> do
          log
            $  "Missing/NoBackup: "
            <> (T.pack $ show p)
            <> " "
            <> missingMsg slds
          return (p, slds)
        Just [] -> do
          log
            $  "Missing/EmptyBackup: "
            <> (T.pack $ show p)
            <> " "
            <> missingMsg slds
          return (p, slds)
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


goodPV (_, pv) = let hasPrecinct = precinct pv /= "" in hasPrecinct

parseFiles log fps =
  fmap (V.fromList . L.filter goodPV) $ S.toList $ parseFiles' log fps

parseFiles'
  :: (T.Text -> IO ()) -> [FilePath] -> S.Serial (Maybe (SLD Int), PrecinctVote)
parseFiles' log fps = do
  let saveErr = either log (const $ return ())
  S.mapMaybe (either (const Nothing) Just)
    $ S.tap (SFL.drainBy saveErr)
    $ S.concatMap parseFile'
    $ S.fromList fps


parseFile'
  :: FilePath -> S.Serial (Either T.Text (Maybe (SLD Int), PrecinctVote))
parseFile' fp = do
  liftIO $ putStrLn $ "loading/parsing \"" ++ fp ++ "\""
  precinctCSV <- liftIO $ fileToBS
    ["county", "Senator", "Senate", "Representative", "Governor"]
    fp
  recsPV <- liftIO $ case CS.decodeByName @PrecinctVote precinctCSV of
    Left err -> E.throw
      $ MiscException ("CSV Header Parse Failure: " <> (T.pack $ show err))
    Right (_, pvRecords) -> return pvRecords
  let addSLD pv = do
        sld <- parseSLDFromPV pv
        return (sld, pv)
      locateError err = T.pack (fp ++ ": " ++ err)
      unfoldF rs = case rs of
        CS.Nil _         _   -> Nothing
        r      `CS.Cons` rs' -> Just (join $ BF.bimap locateError addSLD r, rs')
  S.unfoldr unfoldF recsPV


fileToBS :: [T.Text] -> FilePath -> IO BL.ByteString
fileToBS wordsText fp =
  let eol     = fromIntegral $ TC.ord '\n' :: Word8
      dq      = fromIntegral $ TC.ord '"' :: Word8
      wordsBS = fmap T.encodeUtf8 wordsText
      lines   = S.splitWithSuffix (== eol) SFL.toList
      goodLine :: [Word8] -> Bool
      goodLine x =
        let hasWord bs w = not $ B.null $ snd $ B.breakSubstring w bs
        in  getAny $ mconcat $ fmap (Any . hasWord (B.pack x)) wordsBS
  in  fmap BL.fromChunks
      $ S.toList
      $ S.map B.pack
      $ S.filter goodLine
      $ lines
      $ SF.toBytes fp



data Candidate = Candidate { cName :: !T.Text, cParty :: !T.Text }

type VotesByCandidate = M.Map T.Text Int -- votes by candidate

type VotesByOffice = M.Map T.Text VotesByCandidate

type VotesByPrecinct = M.Map T.Text VotesByOffice

data SLD a = House !Int !a | Senate !Int !a deriving (Show, Eq, Ord, Functor)

isHouse (House _ _) = True
isHouse _           = False

isSenate (Senate _ _) = True
isSenate _            = False

type PrecinctsBySLD = M.Map (SLD ()) (Set.Set PKey)
type SLDsByPrecinct a = M.Map PKey [SLD a]

data ProcessedPrecincts = ProcessedPrecincts { precinctsBySLD :: PrecinctsBySLD
                                             , votesByPrecinct :: VotesByPrecinct
                                             } deriving (Show)

votesBySLDF
  :: SLDsByPrecinct Double -> FL.Fold PrecinctVote [(SLD (), CandidateVote)]
votesBySLDF sldsByP = MR.mapReduceFold
  (MR.Unpack $ precinctVotesToSLDVotes sldsByP)
  (MR.Assign
    (\(sld, pvote) ->
      ( (sld, candOffice pvote, candName pvote, candParty pvote)
      , candVotes pvote
      )
    )
  )
  (MR.foldAndLabel FL.sum
                   (\(sld, co, cn, cp) v -> (sld, CandidateVote co cn cp v))
  )

vbSLDHeader :: T.Text =
  "State Legislative District,Office,Party,Candidate,Votes"

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
  :: SLDsByPrecinct Double -> PrecinctVote -> [(SLD (), CandidateVote)]
precinctVotesToSLDVotes sldsByP pv = case M.lookup (pKey pv) sldsByP of
  Nothing -> [] -- Left $ "No SLDs found for precinct=\"" <> (precinct pv) <> "\"."
  Just slds ->
    let withSLD pv sld = (sldLabel sld, scaleVotes (sldData sld) $ pVote pv)
    in  fmap (withSLD pv) slds

precinctsBySLDF :: FL.Fold (Maybe (SLD Int), PrecinctVote) PrecinctsBySLD
precinctsBySLDF = fmap (fmap Set.fromList . M.fromList) $ MR.mapReduceFold
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
  let officeHas      = hasWord (T.toUpper office)
      isState        = officeHas "STATE"
      isUnitedStates = officeHas "UNITED STATES"
      isHouse        = officeHas "REPRESENTATIVE"
      isSenate       = officeHas "SENATOR" || officeHas "SENATE"
      isSLD          = isState && not isUnitedStates && (isHouse || isSenate)
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
