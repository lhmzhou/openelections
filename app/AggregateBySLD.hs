{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE TupleSections      #-}
{-# LANGUAGE TypeApplications   #-}
module Main where

import qualified Data.Csv                      as C
import           Data.Csv                       ( (.:) )
import qualified Data.List                     as L
import qualified Data.Map                      as M
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
  case FL.foldM allFM withSLDs of
    Left err ->
      E.throw $ MiscException ("SLDByPrecinct error: " <> (T.pack $ show err))
    Right (precinctsBySLDs, vboPrecinct, sldsByPrecinct) -> do
      let allFM = votesBySLDFM sldsByPrecinct
      putStrLn $ "pass 1 complete.\npass 2: votesBySLD and vboHouse, vboSenate"
      votesBySLD <- case FL.foldM allFM pVotesAll of
        Left err -> E.throw
          $ MiscException ("SLD lookup failure: " <> (T.pack $ show err))
        Right x -> return x
      let votesF =
            (,)
              <$> FL.prefilter (isHouse . fst) tvboFromSLDF
              <*> FL.prefilter (isSenate . fst) tvboFromSLDF
          (vboHouse, vboSenate) = FL.fold votesF votesBySLD
      case (vboPrecinct == vboHouse) && (vboHouse == vboSenate) of
        True  -> putStrLn $ "Vote totals match."
        False -> do
          putStrLn $ "Vote totals mismatched!"
          putStrLn $ "by precinct:\n" ++ show vboPrecinct
          putStrLn $ "by house districts:\n" ++ show vboHouse
          putStrLn $ "by senate districts:\n" ++ show vboSenate
          return ()

data Candidate = Candidate { cName :: T.Text, cParty :: T.Text }

type VotesByCandidate = M.Map T.Text Int -- votes by candidate

type VotesByOffice = M.Map T.Text VotesByCandidate

type VotesByPrecinct = M.Map T.Text VotesByOffice

data SLD = NonSLD | House Int | Senate Int deriving (Show, Eq, Ord)

isHouse (House _) = True
isHouse _         = False

isSenate (Senate _) = True
isSenate _          = False


type PrecinctsBySLD = M.Map SLD (S.Set T.Text)
type SLDsByPrecinct = M.Map T.Text PrecinctSLDs

data ProcessedPrecincts = ProcessedPrecincts { precinctsBySLD :: PrecinctsBySLD
                                             , votesByPrecinct :: VotesByPrecinct
                                             } deriving (Show)


data PrecinctSLDs = UnFound | JustSenate Int | JustHouse Int | HouseSenate Int Int

addSLD p NonSLD _ = Left $ "NonSLD precinct should have been filtered out!"
addSLD _ (House x) UnFound = Right $ JustHouse x
addSLD _ (House x) (JustSenate y) = Right $ HouseSenate x y
addSLD p (House x) (JustHouse y) =
  Left
    $  "Error filling out SLDs for precinct=\""
    <> p
    <> "\".  Already had house="
    <> (T.pack $ show y)
    <> " and now found house="
    <> (T.pack $ show x)
addSLD p (House x) (HouseSenate y _) =
  Left
    $  "Error filling out SLDs for precinct=\""
    <> p
    <> "\". Already had house="
    <> (T.pack $ show y)
    <> " and now found house="
    <> (T.pack $ show x)

addSLD _ (Senate x) UnFound       = Right $ JustSenate x
addSLD _ (Senate x) (JustHouse y) = Right $ HouseSenate y x
addSLD p (Senate x) (JustSenate y) =
  Left
    $  "Error filling out SLDs for precinct=\""
    <> p
    <> "\". Already had senate="
    <> (T.pack $ show y)
    <> " and now found senate="
    <> (T.pack $ show x)
addSLD p (Senate x) (HouseSenate _ y) =
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

votesBySLDFM
  :: SLDsByPrecinct
  -> FL.FoldM (Either T.Text) PrecinctVote [(SLD, CandidateVote)]
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

precinctVotesToSLDVotes
  :: SLDsByPrecinct -> PrecinctVote -> Either T.Text [(SLD, CandidateVote)]
precinctVotesToSLDVotes sldsByP pv =
  let withSLD pv sld = (sld, pVote pv)
  in  fmap (fmap $ withSLD pv) $ sldsFromPrecinct sldsByP (precinct pv)

precinctsBySLDF :: FL.Fold (SLD, PrecinctVote) PrecinctsBySLD
precinctsBySLDF = fmap (fmap S.fromList . M.fromList) $ MR.mapReduceFold
  (MR.filterUnpack ((/= NonSLD) . fst))
  (MR.assign fst (precinct . snd))
  (MR.foldAndLabel FL.list (\sld ps -> (sld, ps)))

sldsByPrecinctFM :: FL.FoldM (Either T.Text) (SLD, PrecinctVote) SLDsByPrecinct
sldsByPrecinctFM = fmap M.fromList $ MR.mapReduceFoldM
  (MR.generalizeUnpack $ MR.filterUnpack ((/= NonSLD) . fst))
  (MR.generalizeAssign $ MR.assign (precinct . snd) fst)
  (MR.ReduceFoldM psldF)

psldF :: T.Text -> FL.FoldM (Either T.Text) SLD (T.Text, PrecinctSLDs)
psldF p = FL.FoldM (\pslds sld -> addSLD p sld pslds)
                   (Right UnFound)
                   (\pslds -> return (p, pslds))


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

parseSLDFromPV pv = parseSLD (candOffice $ pVote pv) (districtM pv)

parseSLD :: T.Text -> Maybe Int -> Either T.Text SLD
parseSLD office districtM = do
  let officeHas        = hasWord (T.toUpper office)
      isState          = officeHas "STATE"
      isRepresentative = officeHas "REPRESENTATIVE"
      isSenate         = officeHas "SENATOR"
      isSLD            = isState && (isRepresentative || isSenate)
  case isSLD of
    False -> return NonSLD
    True  -> do
      let dM = districtM <|> (distAtEnd office) -- uses first if possible
      sldFromOffice <- case (isRepresentative, isSenate) of
        (False, False) -> Left $ "Shouldn't be possible!"
        (True , True ) -> Left $ "Bad parse: office=\"" <> office <> "\""
        (True , False) -> return House
        (False, True ) -> return Senate
      case dM of
        Nothing ->
          Left
            $  "Bad district parse (district col was empty): office=\""
            <> office
            <> "\""
        Just d -> Right $ sldFromOffice d



--thinkOne :: V.Vector PrecinctVote -> ProcessedPrecincts 
