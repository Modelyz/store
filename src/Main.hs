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
import Data.Set as Set (Set, delete, empty, insert)
import Data.Text qualified as T (unpack)
import Data.UUID (UUID)
import Message (Message (..), Metadata (..), Payload (..), appendMessage, getFlow, metadata, payload, readMessages, uuid)
import MessageFlow (MessageFlow (..))
import Network.WebSockets qualified as WS
import Options.Applicative

-- port, file
data Options = Options FilePath Host Port

type Client = String

type Host = String

type Port = Int

data State = State
    { pending :: Set Message
    , uuids :: Set UUID
    }
    deriving (Show)

type StateMV = MVar State

emptyState :: State
emptyState = State{pending = Set.empty, Main.uuids = Set.empty}

-- TODO
-- type Pending = Map.Map Int Message

options :: Parser Options
options =
    Options
        <$> strOption (short 'f' <> long "file" <> value "data/messagestore.txt" <> help "Filename of the file containing messages")
        <*> strOption (short 'h' <> long "host" <> value "localhost" <> help "Bind socket to this host. [default: localhost]")
        <*> option auto (short 'p' <> long "port" <> metavar "PORT" <> value 8081 <> help "Bind socket to this port.  [default: 8081]")

routeMessage :: FilePath -> WS.Connection -> Client -> Message -> IO ()
routeMessage msgPath conn client msg = do
    -- route message incoming into store and send to the expected ms
    -- client is the currently connected ms
    case payload msg of
        InitiatedConnection connection -> do
            let remoteUuids = Connection.uuids connection
            esevs <- readMessages msgPath
            let msgs = filter (\e -> uuid (metadata e) `notElem` remoteUuids) esevs
            mapM_ (WS.sendTextData conn . JSON.encode) msgs
            putStrLn $ "\nSent all missing " ++ show (length msgs) ++ " messages to " ++ client
        -- send to ident :
        AddedIdentifierType _ -> do
            Monad.when (client == "ident" && getFlow msg == Requested && from (metadata msg) == "front") $ do
                WS.sendTextData conn $ JSON.encode msg
                putStrLn $ "\nSent to " ++ client ++ " through WS: " ++ show msg
        RemovedIdentifierType _ -> do
            Monad.when (client == "ident" && getFlow msg == Requested && from (metadata msg) == "front") $ do
                WS.sendTextData conn $ JSON.encode msg
                putStrLn $ "\nSent to " ++ client ++ " through WS: " ++ show msg
        ChangedIdentifierType _ _ -> do
            Monad.when (client == "ident" && getFlow msg == Requested && from (metadata msg) == "front") $ do
                WS.sendTextData conn $ JSON.encode msg
                putStrLn $ "\nSent to " ++ client ++ " through WS: " ++ show msg
        AddedIdentifier _ -> do
            Monad.when (client == "ident" && getFlow msg == Requested && from (metadata msg) == "front") $ do
                WS.sendTextData conn $ JSON.encode msg
                putStrLn $ "\nSent to " ++ client ++ " through WS: " ++ show msg
        -- send to studio :
        _ -> do
            Monad.when (client == "studio" && getFlow msg == Processed && from (metadata msg) == "ident") $ do
                WS.sendTextData conn $ JSON.encode msg
                putStrLn $ "\nSent to " ++ client ++ " through WS: " ++ show msg

serverApp :: FilePath -> Chan Message -> StateMV -> WS.ServerApp
serverApp msgPath chan stateMV pending_conn = do
    clientMV <- newMVar ""
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
                    putStrLn "Waiting for msg from the chan"
                    msg <- readChan msChan
                    -- store the name of the client in a thread-local MVar
                    client <- readMVar clientMV
                    putStrLn $ "\nGot msg through the chan from " ++ client ++ ": " ++ show msg
                    routeMessage msgPath conn client msg
                    loop
                )
    -- SERVER MAIN THREAD
    -- handle message coming through websocket from the currently connected microservice
    WS.withPingThread conn 30 (return ()) $
        Monad.forever $ do
            message <- WS.receiveDataMessage conn
            putStrLn $ "\nReceived stuff through websocket: " ++ show message
            case JSON.eitherDecode
                ( case message of
                    WS.Text bs _ -> WS.fromLazyByteString bs
                    WS.Binary bs -> WS.fromLazyByteString bs
                ) of
                Right msg -> do
                    case payload msg of
                        InitiatedConnection _ -> do
                            -- get the name of the connected client
                            let from = T.unpack $ Message.from $ metadata msg
                            _ <- takeMVar clientMV
                            putMVar clientMV from
                            putStrLn $ "Connected client: " ++ from
                        _ -> do
                            appendMessage msgPath msg
                            state <- takeMVar stateMV
                            putMVar stateMV $! update state msg
                            writeChan msChan msg
                            putStrLn $ "Writing to the chan: " ++ show msg
                Left err -> putStrLn $ "\nError decoding incoming message: " ++ err

update :: State -> Message -> State
update state msg =
    case flow (metadata msg) of
        Requested -> case payload msg of
            InitiatedConnection _ -> state
            _ ->
                state
                    { pending = Set.insert msg $ pending state
                    , Main.uuids = Set.insert (uuid (metadata msg)) (Main.uuids state)
                    }
        Processed -> state{pending = Set.delete msg $ pending state}
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
    putStrLn "Old state:"
    print state
    putStrLn "New State:"
    print newState
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
