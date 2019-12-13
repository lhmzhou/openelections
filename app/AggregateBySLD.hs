{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveFunctor      #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE TupleSections      #-}
{-# LANGUAGE TypeApplications   #-}
module Main where

import qualified Data.Csv                      as C
import           Data.Csv                       ( (.:) )
import qualified Data.List                     as L
import qualified Data.Map                      as M
import           Data.Maybe                     ( isJust
                                                , fromJust
                                                )
import qualified Data.Set                      as S
import qualified Data.Text                     as T
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


instance C.FromNamedRecord PrecinctVote where
  parseNamedRecord m = PrecinctVote
                       <$> m .: "county"
                       <*> m .: "precinct"
                       <*> (fmap (readMaybe @Int) $ m .: "district")
                       <*> (CandidateVote
                            <$> m .: "office"
                            <*> m .: "party"
                            <*> m .: "candidate"
                            <*> m .: "votes"
                           )

--instance C.DefaultOrdered PrecinctVote
--    "openelections-data-ga/2018/20181106__ga__general__fulton__precinct.csv"
main :: IO ()
main = do
  let precinctDir    = "openelections-data-ga/2018/"
      electionPrefix = "20181106__ga__general__"
  files <- SD.listDirectory precinctDir
  let filesToProcess = L.filter (electionPrefix `L.isPrefixOf`) files
  pVotesAll <-
    mconcat
      <$> (forM filesToProcess $ \fp -> do
            putStrLn $ "parsing \"" ++ fp ++ "\""
            precinctCSV <- BL.readFile (precinctDir ++ fp)
            case C.decodeByName @PrecinctVote precinctCSV of
              Left err -> E.throw
                $ MiscException ("CSV Parse Failure: " <> (T.pack $ show err))
              Right (_, v) -> return v
          )
  putStrLn $ "parsing SLDs..."
  slds <- case (sequence $ fmap parseSLDFromPV pVotesAll) of
    Left err ->
      E.throw $ MiscException ("SLD Parse Failure: " <> (T.pack $ show err))
    Right x -> return x
  putStrLn $ "Doing pass 1: sldsByPrecinct and vboPrecinct"
  let withSLDs = V.zip slds pVotesAll
      allFM =
        (,,)
          <$> (FL.generalize precinctsBySLDF)
          <*> (FL.generalize tvboFromPrecinctsF)
          <*> sldsByPrecinctFM
  (precinctsBySLDs, vboPrecinct, sldsByPrecinct) <-
    case FL.foldM allFM withSLDs of
      Left err ->
        E.throw $ MiscException ("Pass 1 Failure: " <> (T.pack $ show err))
      Right x -> return x
  putStrLn $ "pass 1 complete.\npass 2: votesBySLD and vboHouse, vboSenate"
  votesBySLD <- case FL.foldM (votesBySLDFM sldsByPrecinct) pVotesAll of
    Left err -> E.throw
      $ MiscException ("Vote re-allocation error: " <> (T.pack $ show err))
    Right x -> return x
  let votesF =
        (,)
          <$> FL.prefilter (isHouse . fst) tvboFromSLDF
          <*> FL.prefilter (isSenate . fst) tvboFromSLDF
      (vboHouse, vboSenate) = FL.fold votesF votesBySLD
  case (vboPrecinct == vboHouse) && (vboHouse == vboSenate) of
    True  -> putStrLn $ "Vote totals match."
    False -> do
      putStrLn
        $  "Vote totals mismatched!"
        ++ "by precinct:\n"
        ++ show vboPrecinct
        ++ "by house districts:\n"
        ++ show vboHouse
        ++ "by senate districts:\n"
        ++ show vboSenate
      return ()

data Candidate = Candidate { cName :: T.Text, cParty :: T.Text }

type VotesByCandidate = M.Map T.Text Int -- votes by candidate

type VotesByOffice = M.Map T.Text VotesByCandidate

type VotesByPrecinct = M.Map T.Text VotesByOffice

data SLD a = House Int a | Senate Int a deriving (Show, Eq, Ord, Functor)

isHouse (House _ _) = True
isHouse _           = False

isSenate (Senate _ _) = True
isSenate _            = False

type PrecinctsBySLD = M.Map (SLD ()) (S.Set T.Text)
type SLDsByPrecinct a = M.Map T.Text [SLD a]

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

sldLabel :: SLD a -> SLD ()
sldLabel = fmap (const ())

sldData :: SLD a -> a
sldData (House  _ x) = x
sldData (Senate _ x) = x

precinctVotesToSLDVotes
  :: SLDsByPrecinct Double
  -> PrecinctVote
  -> Either T.Text [(SLD (), CandidateVote)]
precinctVotesToSLDVotes sldsByP pv = case M.lookup (precinct pv) sldsByP of
  Nothing   -> Left $ "No SLDs found for precinct=\"" <> (precinct pv) <> "\"."
  Just slds -> do
    let withSLD pv sld = (sldLabel sld, scaleVotes (sldData sld) $ pVote pv)
    return $ fmap (withSLD pv) slds

precinctsBySLDF :: FL.Fold (Maybe (SLD Int), PrecinctVote) PrecinctsBySLD
precinctsBySLDF = fmap (fmap S.fromList . M.fromList) $ MR.mapReduceFold
  (MR.filterUnpack (isJust . fst))
  (MR.assign (fromJust . fst) (precinct . snd))
  (MR.foldAndLabel FL.list (\sld ps -> (sldLabel sld, ps)))

sldsByPrecinctFM
  :: FL.FoldM
       (Either T.Text)
       (Maybe (SLD Int), PrecinctVote)
       (SLDsByPrecinct Double)
sldsByPrecinctFM = fmap M.fromList $ MR.mapReduceFoldM
  (MR.generalizeUnpack $ MR.filterUnpack (isJust . fst))
  (MR.generalizeAssign $ MR.assign (precinct . snd) (fromJust . fst))
  (MR.ReduceFoldM f)

f :: T.Text -> FL.FoldM (Either T.Text) (SLD Int) (T.Text, [SLD Double])
f p = fmap (p, ) $ MR.postMapM (sldsVotesToWeights p) (FL.generalize FL.list)

sldsVotesToWeights :: T.Text -> [SLD Int] -> Either T.Text [SLD Double]
sldsVotesToWeights p slds = do
  let hSLDs = filter isHouse slds
      sSLDs = filter isSenate slds
      toWeight total n = (realToFrac n) / (realToFrac total)
  when (L.null hSLDs) $ Left $ p <> " has no state house districts."
  when (L.null sSLDs) $ Left $ p <> " has no state senate districts."
  let hTotal = FL.fold (FL.premap sldData FL.sum) hSLDs
  when (hTotal == 0)
    $  Left
    $  p
    <> " has house votes summing to 0 ("
    <> (T.pack $ show slds)
    <> ")"
  let whSLDs = fmap (fmap (toWeight hTotal)) hSLDs
  let sTotal = FL.fold (FL.premap sldData FL.sum) sSLDs
  when (sTotal == 0)
    $  Left
    $  p
    <> " has senate votes summing to 0 ("
    <> (T.pack $ show slds)
    <> ")"
  let wsSLDs = fmap (fmap (toWeight sTotal)) sSLDs
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
