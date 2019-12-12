{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE TypeApplications   #-}
module Main where

import qualified Data.Csv                      as C
import           Data.Csv                       ( (.:) )
import qualified Data.Map                      as M
import qualified Data.Set                      as S
import qualified Data.Text                     as T
import qualified Data.Char                     as TC
import qualified Data.ByteString.Lazy          as BL
import qualified Data.Vector                   as V
import           Text.Read                      ( readMaybe )

import           Control.Applicative            ( (<|>) )
import           Control.Monad                  ( when )
import qualified Control.Exception             as E
import qualified Control.Foldl                 as FL
import qualified Control.MapReduce             as MR

import           GHC.Generics                   ( Generic )
import           Data.Typeable                  ( Typeable )

data MyException = MiscException T.Text deriving (Show, Typeable)

instance E.Exception MyException

data PrecinctVote =
  PrecinctVote { county :: !T.Text
               , precinct :: !T.Text
               , office :: !T.Text
               , districtM :: !(Maybe Int)
               , party :: !T.Text
               , candidate :: !T.Text
               , votes :: !Int
{-absentee_by_mail :: !Int
               , advance_in_person :: !Int
               , election_day :: !Int
               , provisional :: !Int
-}
               } deriving (Generic, Show)


instance C.FromNamedRecord PrecinctVote where
  parseNamedRecord m = PrecinctVote
                       <$> m .: "county"
                       <*> m .: "precinct"
                       <*> m .: "office"
                       <*> (fmap (readMaybe @Int) $ m .: "district")
                       <*> m .: "party"
                       <*> m .: "candidate"
                       <*> m .: "votes"
{-                       <*> m .: "absentee_by_mail"
                       <*> m .: "advance_in_person"
                       <*> m .: "election_day"
                       <*> m .: "provisional"
-}

instance C.DefaultOrdered PrecinctVote

main :: IO ()
main = do
  precinctCSV <- BL.readFile
    "openelections-data-ga/2018/20181106__ga__general__fulton__precinct.csv"
  pVotes <- case C.decodeByName @PrecinctVote precinctCSV of
    Left err ->
      E.throw $ MiscException ("CSV Parse Failure: " <> (T.pack $ show err))
    Right (_, v) -> return v
  slds <- case (sequence $ fmap parseSLDFromPV pVotes) of
    Left err ->
      E.throw $ MiscException ("SLD Parse Failure: " <> (T.pack $ show err))
    Right x -> return x
  let withSLDs        = V.zip slds pVotes
      precinctsBySLDs = FL.fold precinctsBySLDF withSLDs
  putStrLn $ show $ precinctsBySLDs
  return ()

data Candidate = Candidate { cName :: T.Text, cParty :: T.Text }

type VotesByCandidate = M.Map T.Text Int -- votes by candidate

type VotesByOffice = M.Map T.Text VotesByCandidate

type VotesByPrecinct = M.Map T.Text VotesByOffice

data SLD = NonSLD | House Int | Senate Int deriving (Show, Eq, Ord)

type PrecinctsBySLD = M.Map SLD (S.Set T.Text)

data ProcessedPrecincts = ProcessedPrecincts { precinctsBySLD :: PrecinctsBySLD
                                             , votesByPrecinct :: VotesByPrecinct
                                             } deriving (Show)


precinctsBySLDF :: FL.Fold (SLD, PrecinctVote) PrecinctsBySLD
precinctsBySLDF = fmap (fmap S.fromList . M.fromList) $ MR.mapReduceFold
  (MR.filterUnpack ((/= NonSLD) . fst))
  (MR.assign fst (precinct . snd))
  (MR.foldAndLabel FL.list (\sld ps -> (sld, ps)))



hasWord x y = not . T.null . snd $ T.breakOn y x

distAtEnd :: T.Text -> Maybe Int
distAtEnd = readMaybe . T.unpack . T.takeWhileEnd TC.isDigit . T.strip

parseSLDFromPV pv = parseSLD (office pv) (districtM pv)

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
