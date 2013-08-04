{-# LANGUAGE ScopedTypeVariables, GADTs, OverloadedStrings, DeriveGeneric, GeneralizedNewtypeDeriving, NoMonomorphismRestriction, DeriveDataTypeable, DoAndIfThenElse, TemplateHaskell, QuasiQuotes, TypeFamilies, FlexibleContexts, RankNTypes #-}

module Data.DepFramework
    ( defineGen, runGen, launchFramework, runRootGen
    , publish, publishWithId, getPubId
    , getFile
    , dbGet, dbGetBy, dbSelectList, dbSelectFirst
    , FrameworkCfg(..), Publishable(..), Param(..), Result(..)
    , GenHandle, TopM, DefM
    )
where

import Data.Typeable
import Data.Hashable hiding (hash)
import Data.Aeson hiding (Result)
import Data.Maybe
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.HashSet as Set
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL


import Control.Monad.RWS
import Control.Concurrent
import Control.Monad.Logger
import Control.Monad.Trans.Resource

import GHC.Generics
import Crypto.Hash.SHA1 (hash)
import Unsafe.Coerce
import Text.Printf

import System.Posix.Files
import System.Posix.Types
import System.FilePath

import Database.Persist.TH
import Database.Persist.Sql (SqlPersistT, SqlBackend, ConnectionPool, runSqlPool, runMigration, rawExecute)
import qualified Database.Persist as DB

share [mkPersist sqlSettings, mkMigrate "internalsMig"] [persistLowerCase|
DbLog
    dbtable T.Text
    version Int
    deriving Show
    UniqueTable dbtable
|]

data FunApp where
    FunApp :: forall a b. (Result b) => (FunParam a -> DefM (FunResult b)) -> FunApp

data FunParam a where
    FunParam :: (Param a) => a -> FunParam a

data FunResult a where
    FunResult :: (Result a) => a -> FunResult a

data FunRes where
    FunRes :: forall c . Result c => !c -> FunRes

instance Eq FunRes where
    (FunRes a) == (FunRes b) =
        if typeOf a == typeOf b
        then (unsafeCoerce a) == b
        else False

type FunMap = HM.HashMap T.Text FunApp

type SqlQuery = T.Text

data GenRun where
    GenRun :: forall a b. (Param a, Result b) => GenHandle a b -> a -> GenRun

instance Show GenRun where
    show (GenRun (GenHandle fun) param) =
        T.unpack fun ++ "(" ++ show param ++")"

instance Eq GenRun where
    (GenRun hdl param) == (GenRun hdl2 param2) =
        if sameType
        then and [(unsafeCoerce hdl) == hdl2, show param == show param2]
        else False
        where
          sameType = typeOf hdl == typeOf hdl2

instance Hashable GenRun where
    hashWithSalt salt (GenRun hdl param) =
        salt `hashWithSalt` hdl `hashWithSalt` (show param)

data Dep
   = DepFile !FilePath
   | DepDbTbl !T.Text
   | DepGen !GenRun
   deriving (Show, Eq, Generic)

instance Hashable Dep

type DepMap = HM.HashMap Dep (Set.HashSet GenRun)

class (Show a, Read a, Eq a, Typeable a) => Param a where
instance Param ()
instance Param Char
instance Param T.Text
instance Param Int
instance Param Double
instance Param a => Param [a]
instance (Param a, Param b) => Param (a, b)
instance (Param a, Param b, Param c) => Param (a, b, c)

class (ToJSON a, FromJSON a, Typeable a, Eq a) => Result a where
instance Result T.Text
instance Result Char
instance Result Int
instance Result Double
instance Result a => Result [a]
instance (Result a, Result b) => Result (a, b)
instance (Result a, Result b, Result c) => Result (a, b, c)

class Publishable a where
    publishElem :: T.Text -> a -> SqlM ()

type ResMap = HM.HashMap GenRun FunRes

data TopState
   = TopState
   { ts_funMap :: FunMap
   , ts_depMap :: DepMap
   , ts_resHash :: ResMap
   }

initTopState = TopState HM.empty HM.empty HM.empty

type SqlM = SqlPersistT (LoggingT (ResourceT IO))
type TopM = RWST FrameworkCfg () TopState SqlM
type DefM = RWST GenRun [Dep] () TopM

newtype GenHandle a b = GenHandle { unGenHandle :: T.Text } deriving (Show, Eq, Hashable, Typeable)

recordDep :: Dep -> DefM ()
recordDep dep =
    tell [dep]

defineGen :: forall a b. (Param a, Result b) => T.Text -> (a -> DefM b) -> TopM (GenHandle a b)
defineGen name fun =
    do exists <- gets (HM.lookup name . ts_funMap)
       when (isJust exists) $
            do $(logError) $ T.concat [ "Don't redefine gens! A "
                                      , name
                                      , " is already defined."
                                      ]
               error "Bye."

       modify (\ts -> ts { ts_funMap = (HM.insert name (FunApp (\(FunParam p) ->
                                                                    do r <- fun p
                                                                       return $ FunResult r
                                                               )) $ ts_funMap ts) })
       return $ GenHandle name

runGen :: forall a b. (Param a, Result b) => GenHandle a b -> a -> DefM b
runGen h@(GenHandle name) param =
    do recordDep $ DepGen (GenRun h param)
       lift $ runRootGen h param

publish :: forall b. (Result b, Publishable b) => b -> DefM T.Text
publish = publish' Nothing

publishWithId :: forall b. (Result b, Publishable b) => T.Text -> b -> DefM T.Text
publishWithId h = publish' (Just h)

publish' :: forall b. (Result b, Publishable b) => Maybe T.Text -> b -> DefM T.Text
publish' def res =
    do pubId <- getPubId
       let pHash = fromMaybe pubId def
       lift $ lift $ publishElem pHash res
       return pHash

getPubId :: DefM T.Text
getPubId =
    do (GenRun (GenHandle name) param) <- ask
       let pHash = hashOf $ T.concat [ name, T.pack $ show param ]
       return pHash

hashOf :: T.Text -> T.Text
hashOf = T.pack . toHex . hash . T.encodeUtf8
         where
           toHex bytes = BS.unpack bytes >>= printf "%02x"

runRootGen :: forall a b. (Param a, Result b) => GenHandle a b -> a -> TopM b
runRootGen handle@(GenHandle name) param =
    do fm <- gets ts_funMap
       rh <- gets ts_resHash
       dm <- gets ts_depMap
       case HM.lookup name fm of
         Just (FunApp fun) ->
             do let me = GenRun handle param
                case HM.lookup me rh of
                  Just (FunRes val) ->
                      return (unsafeCoerce val)
                  Nothing ->
                      do $(logInfo) $ T.concat ["Evaluating ", (showT me)]
                         (FunResult res, deps) <- evalRWST (fun (unsafeCoerce $ FunParam param)) me ()
                         $(logDebug) $ T.concat ["Got: ", (showT me), " ===> ", showT $ encode res]
                         let old = oldDeps dm me
                             notNeeded = Set.toList (old `Set.difference` (Set.fromList deps))
                         modify (\ts ->
                                 ts { ts_depMap = foldl (\hm dep ->
                                                             HM.adjust (\s -> Set.delete me s) dep hm
                                                        ) (ts_depMap ts) notNeeded
                                    })
                         when (length notNeeded /= 0) $
                            do $(logInfo) $ T.concat ["No longer needed deps: ", showT notNeeded]
                               gcDeps

                         modify (\ts ->
                                 ts { ts_depMap = foldl (\hm dep ->
                                                             HM.insertWith (\_ old -> Set.insert me old) dep (Set.singleton me) hm
                                                        ) (ts_depMap ts) deps
                                    })
                         let wrapped = FunRes res
                         modify (\ts -> ts { ts_resHash = HM.insert me wrapped (ts_resHash ts) })
                         return (unsafeCoerce res)
         Nothing ->
             error "Invalid handle?!"
    where
      oldDeps dm me =
          HM.foldlWithKey' (\set dep val ->
                             if Set.member me val
                             then Set.insert dep set
                             else set
                        ) Set.empty dm

gcDeps :: TopM ()
gcDeps =
    do dp <- gets ts_depMap
       let (delRun, delKey) =
               HM.foldlWithKey' (\(runSet, keySet) dep val ->
                                     if Set.null val
                                     then case dep of
                                            (DepGen genRun) ->
                                                (Set.insert genRun runSet, Set.insert dep keySet)
                                            _ ->
                                                (runSet, Set.insert dep keySet)
                                     else (runSet, keySet)
                                ) (Set.empty, Set.empty) dp
       $(logDebug) $ T.concat [ "Will garbage-collect those deps: "
                              , showT delRun
                              , " and those cached vals: "
                              , showT delRun
                              ]
       let delRun' = Set.toList delRun
           delKey' = Set.toList delKey
       mapM_ (\k -> modify (\ts -> ts { ts_depMap = HM.delete k (ts_depMap ts) })) delKey'
       mapM_ (\k -> modify (\ts -> ts { ts_resHash = HM.delete k (ts_resHash ts) })) delRun'

       dp' <- gets ts_depMap
       modify (\ts ->
                   ts { ts_depMap = foldl (\hm depKey ->
                                               foldl (\hm' r -> HM.adjust (\s -> Set.delete r s) depKey hm') hm delRun'
                                          ) (ts_depMap ts) (HM.keys $ ts_depMap ts)
                      }
              )

evalDep :: Dep -> Set.HashSet GenRun -> TopM ()
evalDep dk skip =
    do dp <- gets ts_depMap
       case fmap (\x -> Set.difference x skip) $ HM.lookup dk dp of
         Just genApps ->
             if Set.null genApps
             then $(logDebug) $ T.concat [ "Done with "
                                         , showT dk
                                         ]
             else do $(logDebug) $ T.concat [ "Deps for Key "
                                            , showT dk
                                            , ": "
                                            , showT genApps
                                            ]
                     let next = head $ Set.toList genApps
                     runChildrenIfChanged next
                     evalDep dk (Set.insert next skip)
         Nothing ->
             $(logDebug) $ T.concat [ "Nobody depends on "
                                    , showT dk
                                    , ". That's good!"
                                    ]

runChildrenIfChanged :: GenRun -> TopM ()
runChildrenIfChanged r@(GenRun hdl@(GenHandle name) param) =
    do oldVal <- gets (HM.lookup r . ts_resHash)
       modify (\ts -> ts { ts_resHash = HM.delete r (ts_resHash ts) })
       res <- runRootGen hdl param
       if oldVal /= (Just $ FunRes res)
       then evalDep (DepGen r) Set.empty
       else return ()

getFile :: FilePath -> DefM BS.ByteString
getFile fp =
    do recordDep $ DepFile fp
       bs <- liftIO $ BS.readFile fp
       return bs

dummyFromKey :: DB.KeyBackend SqlBackend v -> Maybe v
dummyFromKey _ = Nothing

dummyFromUnique :: DB.Unique v -> Maybe v
dummyFromUnique _ = Nothing

dummyFromFilts :: [DB.Filter v] -> Maybe v
dummyFromFilts _ = Nothing

dbSelectList :: (DB.PersistEntity val, DB.PersistEntityBackend val ~ SqlBackend) => [DB.Filter val] -> [DB.SelectOpt val] -> DefM [DB.Entity val]
dbSelectList filters opts =
    do let t = DB.entityDef $ dummyFromFilts filters
       recordDep $ DepDbTbl (DB.unDBName $ DB.entityDB t)
       lift $ lift $ DB.selectList filters opts

dbSelectFirst :: (DB.PersistEntity val, DB.PersistEntityBackend val ~ SqlBackend) => [DB.Filter val] -> [DB.SelectOpt val] -> DefM (Maybe (DB.Entity val))
dbSelectFirst filters opts =
    do let t = DB.entityDef $ dummyFromFilts filters
       recordDep $ DepDbTbl (DB.unDBName $ DB.entityDB t)
       lift $ lift $ DB.selectFirst filters opts

dbGetBy :: (DB.PersistEntityBackend val ~ SqlBackend, DB.PersistEntity val) => DB.Unique val -> DefM (Maybe (DB.Entity val))
dbGetBy unique =
    do let t = DB.entityDef $ dummyFromUnique unique
       recordDep $ DepDbTbl (DB.unDBName $ DB.entityDB t)
       lift $ lift $ DB.getBy unique

dbGet :: (DB.PersistEntityBackend val ~ SqlBackend,
            DB.PersistEntity val) => DB.Key val -> DefM (Maybe val)
dbGet key =
    do let t = DB.entityDef $ dummyFromKey key
       recordDep $ DepDbTbl (DB.unDBName $ DB.entityDB t)
       lift $ lift $ DB.get key

data DepState
   = DepState
   { ds_filemap :: HM.HashMap FilePath EpochTime
   , ds_dbmap :: HM.HashMap T.Text Int
   , ds_triggers :: Set.HashSet T.Text
   }

depRunner :: DepState -> TopM DepState
depRunner depState =
    do dp' <- gets ts_depMap
       let dpKeys = filter depFilter $ HM.keys dp'
       foldM depFold depState dpKeys
    where
      depFilter (DepGen _) = False
      depFilter _ = True

      depFold ds@(DepState { ds_filemap = fileMap}) dk@(DepFile fp) =
          do stat <- liftIO $ getFileStatus fp
             let m = modificationTime stat
                 old = HM.lookup fp fileMap
             if old /= (Just m)
             then do evalDep dk Set.empty
                     return $ ds { ds_filemap = (HM.insert fp m fileMap) }
             else return ds

      depFold ds@(DepState fileMap dbmap triggers) dk@(DepDbTbl tbl) =
          do triggers' <-
                 if Set.member tbl triggers
                 then return triggers
                 else do dbVal <- lift $ DB.getBy (UniqueTable tbl)
                         case dbVal of
                           Nothing ->
                               do lift $ DB.insert $ DbLog tbl 0
                                  lift $ rawExecute (triggerTpl tbl "update") []
                                  lift $ rawExecute (triggerTpl tbl "insert") []
                                  lift $ rawExecute (triggerTpl tbl "delete") []
                                  $(logDebug) $ T.concat [ "Created triggers for ", tbl]
                           Just entity ->
                               do let vers = dbLogVersion $ DB.entityVal entity
                                  $(logDebug) $ T.concat [ "Trigger for "
                                                         , tbl
                                                         , " propably already exists. Version is "
                                                         , showT vers
                                                         ]
                         return (Set.insert tbl triggers)

             dbVal <- lift $ DB.getBy (UniqueTable tbl)
             let tblVers =
                     case dbVal of
                       Nothing -> -1
                       Just entity ->
                           dbLogVersion $ DB.entityVal entity
                 old = HM.lookup tbl dbmap
             if old /= (Just tblVers)
             then do $(logDebug) $ T.concat [ "Version of dbTable ", tbl, " changed from ", showT old
                                            , " to ", showT tblVers
                                            ]
                     evalDep dk Set.empty
                     return $ (DepState fileMap (HM.insert tbl tblVers dbmap) triggers')
             else return (DepState fileMap dbmap triggers')

      depFold _ _ = error "Not implemented depKey!"

triggerTpl tbl ty = T.concat [ "CREATE TRIGGER "
                             , tbl
                             , "_", ty, " AFTER ", T.toUpper ty, " ON "
                             , tbl
                             , " FOR EACH ROW BEGIN "
                             , "UPDATE db_log SET version = version + 1 WHERE dbtable = '"
                             , tbl
                             , "';"
                             , "END;"]

data FrameworkCfg
   = FrameworkCfg
   { fc_connPool :: ConnectionPool
   }

showT = T.pack . show

launchFramework cfg migration rootAction =
    runResourceT $
    runStdoutLoggingT $
    (flip runSqlPool) (fc_connPool cfg) $
    do runMigration internalsMig
       runMigration migration
       _ <- execRWST body cfg initTopState
       $(logInfo) "Framework terminated."
       return ()
    where
      body =
          do rootAction
             loop (DepState HM.empty HM.empty Set.empty)
      loop st =
          do nst <- depRunner st
             liftIO $ threadDelay (10 * 1000000) -- 10 sec
             loop nst
