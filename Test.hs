{-# LANGUAGE OverloadedStrings, TemplateHaskell, QuasiQuotes, TypeFamilies, FlexibleContexts, EmptyDataDecls, GADTs #-}
import Data.DepFramework
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Database.Persist.MySQL
import Database.Persist
import Database.Persist.TH
import Database.Persist.Sql (runSqlPool)

share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
User
    name T.Text
    age Int
    deriving Show
|]

test =
    defineGen "fileLoader" $ \() ->
        do someBS <- getFile "test.txt"
           return $ T.decodeUtf8 someBS

test2 fileG realG =
    defineGen "root" $ \() ->
        do someId <- runGen fileG ()
           pub <- runGen realG someId
           publishWithId "root" $ T.concat [someId, " --> ", pub]

test3 fortytwoG dbG =
    defineGen "concater" $ \(random) ->
        do fortytwo <- runGen fortytwoG ()
           users <- runGen dbG ()
           return $ T.concat (["HALLO: ", random, fortytwo, " USERS: "] ++ users)

test4 =
    defineGen "42giver" $ \() ->
        do someBS <- getFile "test2.txt"
           publish $ T.decodeUtf8 someBS

test5 =
    defineGen "databaser" $ \() ->
        do entites <- dbSelectList [] []
           return $ map (userName . entityVal) entites

main =
    do pool <- createMySQLPool (defaultConnectInfo { connectHost = "127.0.0.1" } ) 5
       main' pool

main' pool =
    launchFramework (FrameworkCfg "out") pool migrateAll $
    do fileG <- test
       fortytwoG <- test4
       dbG <- test5
       realG <- test3 fortytwoG dbG
       fullG <- test2 fileG realG
       _ <- runRootGen fullG ()
       return undefined
