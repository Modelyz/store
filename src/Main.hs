{-# LANGUAGE OverloadedStrings #-}

import Connection (Connection (..))
import Control.Concurrent (
    Chan,
    MVar,
    dupChan,
    forkIO,
    newChan,
    newMVar,
    putMVar,
    readChan,
    readMVar,
    takeMVar,
    writeChan,
 )
import Control.Monad qualified as Monad (forever, when)
import Control.Monad.Fix (fix)
import Data.Aeson qualified as JSON
import Data.List ()
import Data.Map.Strict as Map (Map, delete, empty, insert)
import Data.Set as Set (Set, empty, insert)
import Message (Message (..), Payload (..), addVisited, appendMessage, creator, dropLastVisited, getFlow, metadata, payload, readMessages)
import MessageFlow (MessageFlow (..))
import MessageId (MessageId, messageId)
import Metadata (Metadata (Metadata), Origin (..), flow, from, uuid, when)
import Network.WebSockets qualified as WS
import Options.Applicative

-- port, file
data Options = Options FilePath Host Port

type Client = Origin

type Host = String

type Port = Int

data State = State
    { pending :: Map MessageId Message
    , uuids :: Set MessageId
    }
    deriving (Show)

type StateMV = MVar State

emptyState :: State
emptyState = State{pending = Map.empty, Main.uuids = Set.empty}

options :: Parser Options
options =
    Options
        <$> strOption (short 'f' <> long "file" <> value "data/messagestore.txt" <> help "Filename of the file containing messages")
        <*> strOption (short 'h' <> long "host" <> value "localhost" <> help "Bind socket to this host. [default: localhost]")
        <*> option auto (short 'p' <> long "port" <> metavar "PORT" <> value 8081 <> help "Bind socket to this port.  [default: 8081]")

syncBackMessage :: WS.Connection -> Client -> Message -> IO ()
syncBackMessage conn client msg = do
    -- sync back the messages to an empty service
    case client of
        Ident -> case payload msg of
            InitiatedConnection _ -> return ()
            -- send to ident :
            AddedIdentifierType _ -> WS.sendTextData conn $ JSON.encode $ dropLastVisited msg
            RemovedIdentifierType _ -> WS.sendTextData conn $ JSON.encode $ dropLastVisited msg
            ChangedIdentifierType _ _ -> WS.sendTextData conn $ JSON.encode $ dropLastVisited msg
            AddedIdentifier _ -> WS.sendTextData conn $ JSON.encode $ dropLastVisited msg
            _ -> return ()
        Studio -> WS.sendTextData conn $ JSON.encode $ dropLastVisited msg
        _ -> return ()

routedMessage :: WS.Connection -> Client -> Message -> IO ()
routedMessage conn client msg = do
    -- route message from store to the correct service
    -- client is the currently connected ms
    case client of
        Ident -> case getFlow msg of
            Requested -> case creator msg of
                Front ->
                    case payload msg of
                        InitiatedConnection _ -> return ()
                        -- send to ident :
                        AddedIdentifierType _ -> do
                            WS.sendTextData conn $ JSON.encode $ addVisited Store msg
                            putStrLn $ "Sent to " ++ show client ++ " through WS"
                        RemovedIdentifierType _ -> do
                            WS.sendTextData conn $ JSON.encode $ addVisited Store msg
                            putStrLn $ "Sent to " ++ show client ++ " through WS"
                        ChangedIdentifierType _ _ -> do
                            WS.sendTextData conn $ JSON.encode $ addVisited Store msg
                            putStrLn $ "Sent to " ++ show client ++ " through WS"
                        AddedIdentifier _ -> do
                            WS.sendTextData conn $ JSON.encode $ addVisited Store msg
                            putStrLn $ "Sent to " ++ show client ++ " through WS"
                        _ -> return ()
                _ -> return ()
            _ -> return ()
        Studio -> case getFlow msg of
            Requested -> case creator msg of
                Front -> case payload msg of
                    InitiatedConnection _ -> return ()
                    AddedIdentifierType _ -> return ()
                    RemovedIdentifierType _ -> return ()
                    ChangedIdentifierType _ _ -> return ()
                    AddedIdentifier _ -> return ()
                    _ -> do
                        WS.sendTextData conn $ JSON.encode $ addVisited Store msg
                        putStrLn $ "Sent to " ++ show client ++ " through WS"
                _ -> return ()
            Processed -> case creator msg of
                Studio -> do
                    WS.sendTextData conn $ JSON.encode $ addVisited Store msg
                    putStrLn $ "Sent to " ++ show client ++ " through WS"
                Ident -> do
                    WS.sendTextData conn $ JSON.encode $ addVisited Store msg
                    putStrLn $ "Sent to " ++ show client ++ " through WS"
                _ -> return ()
            _ -> return ()
        _ -> return ()

serverApp :: FilePath -> Chan Message -> StateMV -> WS.ServerApp
serverApp msgPath chan stateMV pending_conn = do
    clientMV <- newMVar None
    msChan <- dupChan chan
    -- accept a new connexion
    conn <- WS.acceptRequest pending_conn
    _ <-
        -- SERVER WORKER THREAD (one per client thread)
        -- wait for new messages coming from other microservices through the chan
        -- and send them to the currently connected microservice
        forkIO $
            fix
                ( \loop -> do
                    msg <- readChan msChan
                    -- store the name of the client in a thread-local MVar
                    client <- readMVar clientMV
                    putStrLn $ "SERVER WORKER THREAD for " ++ show client ++ " received msg through the chan:\n" ++ show msg
                    routedMessage conn client msg
                    loop
                )
    -- SERVER MAIN THREAD
    -- handle message coming through websocket from the currently connected microservice
    WS.withPingThread conn 30 (return ()) $
        Monad.forever $ do
            message <- WS.receiveDataMessage conn
            putStrLn $ "SERVER MAIN THREAD received through websocket:\n" ++ show message
            case JSON.eitherDecode
                ( case message of
                    WS.Text bs _ -> WS.fromLazyByteString bs
                    WS.Binary bs -> WS.fromLazyByteString bs
                ) of
                Right msg -> do
                    st' <- readMVar stateMV
                    case payload msg of
                        InitiatedConnection connection -> do
                            -- get the name of the connected client
                            let from = creator msg
                            _ <- takeMVar clientMV
                            putMVar clientMV from
                            putStrLn $ "Connected client: " ++ show from
                            let remoteUuids = Connection.uuids connection
                            messages <- readMessages msgPath
                            let msgs = filter (\e -> (messageId . metadata) e `notElem` remoteUuids) messages
                            client <- readMVar clientMV
                            mapM_ (syncBackMessage conn client) msgs
                            putStrLn $ "Sent all missing messages to " ++ show from
                            -- send the InitiatedConnection terminaison to signal the sync is over
                            (WS.sendTextData conn . JSON.encode) $
                                Message
                                    (Metadata{uuid = uuid $ metadata msg, Metadata.when = when $ metadata msg, Metadata.from = [Store], Metadata.flow = Processed})
                                    (InitiatedConnection (Connection{lastMessageTime = 0, Connection.uuids = Set.empty}))
                        _ -> Monad.when (messageId (metadata msg) `notElem` Main.uuids st') $ do
                            appendMessage msgPath msg
                            state <- takeMVar stateMV
                            putMVar stateMV $! update state msg
                            writeChan msChan msg
                            putStrLn "Writing to the chan"
                Left err -> putStrLn $ "### ERROR ### decoding incoming message:\n" ++ err

update :: State -> Message -> State
update state msg =
    case flow (metadata msg) of
        Requested -> case payload msg of
            InitiatedConnection _ -> state
            _ ->
                state
                    { pending = Map.insert (messageId (metadata msg)) msg $ pending state
                    , Main.uuids = Set.insert (messageId $ metadata msg) (Main.uuids state)
                    }
        Processed ->
            state
                { pending = Map.delete (messageId (metadata msg)) $ pending state
                , Main.uuids = Set.insert (messageId $ metadata msg) (Main.uuids state)
                }
        Error _ -> state

serve :: Options -> IO ()
serve (Options storePath listHost listenPort) = do
    stateMV <- newMVar emptyState -- application state-
    chan <- newChan
    -- Reconstruct the state
    putStrLn "Reconstructing the State..."
    msgs <- readMessages storePath
    state <- takeMVar stateMV
    let newState = foldl update state msgs -- TODO foldr or strict foldl ?
    putMVar stateMV newState
    putStrLn $ "STATE:\n" ++ show newState
    -- listen for clients
    putStrLn $ "Modelyz Store, serving from localhost:" ++ show listenPort ++ "/"
    WS.runServerWithOptions WS.defaultServerOptions{WS.serverHost = listHost, WS.serverPort = listenPort} (serverApp storePath chan stateMV)

main :: IO ()
main =
    serve =<< execParser opts
  where
    opts =
        info
            (options <**> helper)
            ( fullDesc
                <> progDesc "The central source of all your events"
                <> header "Modelyz Store"
            )
