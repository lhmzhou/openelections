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
import qualified Data.FuzzySet                 as FS
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

cleanTextField = T.unwords . T.words . T.strip

instance C.FromNamedRecord PrecinctVote where
  parseNamedRecord m = PrecinctVote
                       <$> (fmap cleanTextField $ m .: "county")
                       <*> (fmap (cleanTextField . T.filter (/= ' ') . T.toUpper) $ m .: "precinct")
                       <*> (fmap (readMaybe @Int . L.filter TC.isDigit) $ m .: "district")
                       <*> (CandidateVote
                            <$> (fmap cleanTextField $ m .: "office")
                            <*> (fmap cleanTextField $ m .: "party")
                            <*> (fmap cleanTextField $ m .: "candidate")
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
                               , useFuzzyMatches :: Bool
                               }

allFilesInDirWithPrefix :: FilePath -> String -> IO [FilePath]
allFilesInDirWithPrefix dir prefix = do
  let addDir x = dir ++ x
  allFiles <- SD.listDirectory dir
  return $ fmap addDir $ L.filter (L.isPrefixOf prefix) allFiles


-- order matters in the backup dictionary.  Should something be found in
-- an earlier and later file, the earlier file will be preferred.
-- So put more recent data first and then add data going further back.
gaConfig = StateConfig
  (do
    allGeneral2018 <- allFilesInDirWithPrefix "openelections-data-ga/2018/"
                                              "20181106__ga__general__"
    allPrimary2018 <- allFilesInDirWithPrefix "openelections-data-ga/2018/"
                                              "20181106__ga__primary__"
    allGeneral2016 <- allFilesInDirWithPrefix "openelections-data-ga/2016/"
                                              "20161108__ga__general__"
    return (allGeneral2018, allPrimary2018 ++ allGeneral2016)
  )
  "results/GA_VotesByStateLegislativeDistrict.csv"
  (Just "logs/GA.log")
  False


txConfig =
  let
    getFiles = do
      allPrimary2018 <- allFilesInDirWithPrefix
        "openelections-data-tx/2018/counties/"
        "20180306__tx__primary__"
      allGeneral2014 <- allFilesInDirWithPrefix "openelections-data-tx/2014/"
                                                "20141104__tx__general__"
      let general2018 =
            ["openelections-data-tx/2018/20181106__tx__general__precinct.csv"]
          general2016 =
            ["openelections-data-tx/2016/20161108__tx__general__precinct.csv"]
      return (general2018, allPrimary2018 ++ general2016 ++ allGeneral2014)
  in  StateConfig getFiles
                  "results/TX_VotesByStateLegislativeDistrict.csv"
                  (Just "logs/TX.log")
                  False

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
  True

paConfig = StateConfig
  (do
    let elexFiles =
          ["openelections-data-pa/2018/20181106__pa__general__precinct.csv"]
        backupFiles =
          [ "openelections-data-pa/2016/20161108__pa__general__precinct.csv"
          , "openelections-data-pa/2014/20141104__pa__general__precinct.csv"
          ]
    return (elexFiles, backupFiles)
  )
  "results/PA_VotesByStateLegislativeDistrict.csv"
  (Just "logs/PA.log")
  True


main :: IO ()
main = do
  let stateConfig = gaConfig
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
  putStrLn $ "constructing backup dictionary of precinct -> SLD mappings"
    -- We do this one file at a time so we can prefer newer to older data
    -- also might make concurrency a possibility
  let
    makeBackupDict fp = do
      parsed <- parseFiles log [fp]
      return $ FL.fold sldsByPrecinctF parsed
    backupDictMerge slds1 slds2 =
      let (house1, senate1) = L.partition isHouse slds1
          (house2, senate2) = L.partition isHouse slds2
          house             = if not (L.null house1) then house1 else house2
          senate            = if not (L.null senate1) then senate1 else senate2
      in  house ++ senate
    foldDictsF =
      FL.Fold (\m1 m2 -> M.unionWith backupDictMerge m1 m2) M.empty id
  backupSLDsBP <- fmap (FL.fold foldDictsF)
    $ traverse makeBackupDict backupPrecFiles
  putStrLn
    $ "Doing pass 1: sldsByPrecinct, vboPrecinct and primary precinct -> SLDs dictionary"
  let allF =
        (,,) <$> precinctsBySLDF <*> tvboFromPrecinctsF <*> sldsByPrecinctF
      (precinctsBySLDs, vboPrecinct, sldsByPrecinct) =
        FL.fold allF parsedElection
  putStrLn $ "Backfilling primary dictionary:"
  updatedSLDsByPrecinct <- backfillDictionary log
                                              (useFuzzyMatches stateConfig)
                                              sldsByPrecinct
                                              backupSLDsBP
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
      isUS        = hasWord "United" o || hasWord "U.S." o
      isGov       = hasWord "Governor" o
      isUS_Senate = isUS && (hasWord "Senator" o || hasWord "Senate" o)
      isUS_House  = isUS && hasWord "Representative" o
      isState_Senate = (not isUS) && (hasWord "Senator" o || hasWord "Senate" o)
      isState_House = (not isUS) && hasWord "Representative" o
  in  isGov || isUS_Senate || isUS_House || isState_Senate || isState_House

outputCompare :: (SLD (), CandidateVote) -> (SLD (), CandidateVote) -> Ordering
outputCompare (sldA, (CandidateVote oA nA _ _)) (sldB, (CandidateVote oB nB _ _))
  = compare oA oB <> compare sldA sldB <> compare nA nB

backfillDictionary
  :: (T.Text -> IO ())
  -> Bool
  -> SLDsByPrecinct Double
  -> SLDsByPrecinct Double
  -> IO (SLDsByPrecinct Double)
backfillDictionary log useFuzzy electionD backupD = do
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
  let printList = T.intercalate "\n" . fmap (T.pack . show)
  log $ "House from backup:\n" <> printList withBackupH
  log $ "Senate from backup:\n" <> printList withBackupS
  log $ "Multi-House:\n" <> printList multiH
  log $ "Multi-Senate:\n" <> printList multiS

  let
    fromBackupM =
      M.unionWith (<>) (M.fromList withBackupH) (M.fromList withBackupS) -- in case same precinct is on both lists
    newSLDsByPrecinct        = M.union fromBackupM electionD -- prefer updated
    (stillNeedH, stillNeedS) = FL.fold
      (   (,)
      <$> FL.prefilter (missingHouse . snd) FL.list
      <*> FL.prefilter (missingSenate . snd) FL.list
      )
      (M.toList newSLDsByPrecinct)
    filterNumberKeysAndEmptySLDs = M.filterWithKey
      (\(PKey _ p) l ->
        (not $ L.null l) && (not $ T.null $ T.dropWhile TC.isDigit p)
      )
    backupForFuzzy isType =
      filterNumberKeysAndEmptySLDs $ fmap (L.filter isType) backupD
    fuzzySetsByCountyH =
      fmap FS.fromList
        $ M.fromListWith (<>)
        $ fmap (\(PKey c p) -> (c, [p]))
        $ M.keys
        $ backupForFuzzy isHouse
    fuzzySetsByCountyS =
      fmap FS.fromList
        $ M.fromListWith (<>)
        $ fmap (\(PKey c p) -> (c, [p]))
        $ M.keys
        $ backupForFuzzy isSenate
    tryMatch fuzzyMap (PKey c p) = M.lookup c fuzzyMap >>= flip FS.getOne p
    fuzzyH = filter (isJust . snd) $ zip stillNeedH $ fmap
      (tryMatch fuzzySetsByCountyH . fst)
      stillNeedH
    fuzzyS = filter (isJust . snd) $ zip stillNeedS $ fmap
      (tryMatch fuzzySetsByCountyS . fst)
      stillNeedS

    printFuzzy ((pk, _), Just fp) = (T.pack $ show pk) <> " matched " <> fp
    printFuzzyList = T.intercalate "\n" . fmap printFuzzy
    addFromFuzzy isType ((pk, slds), Nothing) = return (pk, slds)
    addFromFuzzy isType ((pk@(PKey c _), slds), Just fp) =
      fmap (\(_, slds') -> (pk, slds')) $ addFromBackup isType (PKey c fp, slds)
  fuzzySLDsByPrecinct <- case useFuzzy of
    False -> return M.empty
    True  -> do
      log $ printFuzzyList fuzzyH
      log $ printFuzzyList fuzzyS
      withFuzzyH <- traverse (addFromFuzzy isHouse) fuzzyH
      withFuzzyS <- traverse (addFromFuzzy isSenate) fuzzyS
      return $ M.unionWith (<>) (M.fromList withFuzzyH) (M.fromList withFuzzyS)

  return $ M.union fuzzySLDsByPrecinct newSLDsByPrecinct


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

sldsByPrecinctF
  :: FL.Fold (Maybe (SLD Int), PrecinctVote) (SLDsByPrecinct Double)
sldsByPrecinctF = fmap M.fromList $ MR.mapReduceFold
  (MR.filterUnpack (isJust . fst))
  (MR.assign (pKey . snd) (fromJust . fst))
  (MR.ReduceFold f)

f :: PKey -> FL.Fold (SLD Int) (PKey, [SLD Double])
f p = fmap (\l -> (p, (sldsVotesToWeights p) l)) FL.list


-- TODO: sum to 0 -> equal weights??
sldsVotesToWeights :: PKey -> [SLD Int] -> [SLD Double]
sldsVotesToWeights p slds
  = let
      toWeight total n = (realToFrac n) / (realToFrac total)
      weightSLDs p seatType slds = case sumDistrictData slds of
        []  -> []
        [x] -> [fmap (const 1) x]
        xs ->
          let
            t = FL.fold (FL.premap sldData FL.sum) xs
            g =
              if t == 0 then const (1 / realToFrac (length xs)) else toWeight t
          in
            fmap (fmap g) xs
      whSLDs = weightSLDs p "House" $ filter isHouse slds
      wsSLDs = weightSLDs p "Senate" $ filter isSenate slds
    in
      whSLDs ++ wsSLDs


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
