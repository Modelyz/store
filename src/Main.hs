{-# LANGUAGE OverloadedStrings #-}

import Connection (uuids)
import Control.Concurrent (
    Chan,
    MVar,
    dupChan,
    forkIO,
    newChan,
    newMVar,
    putMVar,
    readChan,
    takeMVar,
    writeChan,
 )
import Control.Monad (forever, unless, when)
import Control.Monad.Fix (fix)
import Data.Aeson qualified as JSON
import Data.List ()
import Message (Message (..), Payload (InitiatedConnection), appendMessage, getFlow, isType, metadata, payload, readMessages, setCreator, setFlow, uuid)
import MessageFlow (MessageFlow (..))
import Network.WebSockets qualified as WS
import Options.Applicative

-- port, file
data Options = Options FilePath Host Port

type NumClient = Int

type Host = String

type Port = Int

-- TODO
-- type Pending = Map.Map Int Message

options :: Parser Options
options =
    Options
        <$> strOption (short 'f' <> long "file" <> value "data/messagestore.txt" <> help "Filename of the file containing events")
        <*> strOption (short 'h' <> long "host" <> value "localhost" <> help "Bind socket to this host. [default: localhost]")
        <*> option auto (short 'p' <> long "port" <> metavar "PORT" <> value 8081 <> help "Bind socket to this port.  [default: 8081]")

wsApp :: FilePath -> Chan (NumClient, Message) -> MVar NumClient -> WS.ServerApp
wsApp f chan ncMV pending_conn = do
    mschan <- dupChan chan
    -- accept a new connexion
    conn <- WS.acceptRequest pending_conn
    -- increment the sequence of client microservices
    nc <- takeMVar ncMV
    putMVar ncMV (nc + 1)
    putStrLn $ "Microservice " ++ show nc ++ " connected"
    -- wait for new messages coming from other microservices through the chan
    -- and send them to the currently connected microservice
    _ <-
        forkIO $
            fix
                ( \loop -> do
                    (n, ev) <- readChan mschan
                    when (n /= nc && not (ev `isType` "InitiatedConnection")) $ do
                        putStrLn $ "\nThread " ++ show nc ++ " got stuff through the chan from connected microservice " ++ show n ++ ": " ++ show ev
                        -- TODO: filter what we send back to other microservices?
                        WS.sendTextData conn $ JSON.encode [ev]
                        putStrLn $ "\nSent to client " ++ show nc ++ " through WS: " ++ show ev
                    loop
                )
    -- handle messages coming through websocket from the currently connected microservice
    WS.withPingThread conn 30 (return ()) $
        forever $ do
            putStrLn $ "\nWaiting for new messages from microservice " ++ show nc
            messages <- WS.receiveDataMessage conn
            putStrLn $ "\nReceived stuff through websocket from microservice " ++ show nc ++ ". Handling it : " ++ show messages
            case JSON.eitherDecode
                ( case messages of
                    WS.Text bs _ -> WS.fromLazyByteString bs
                    WS.Binary bs -> WS.fromLazyByteString bs
                ) of
                Right evs -> handleMessage f conn nc mschan evs
                Left err -> putStrLn $ "\nError decoding incoming message: " ++ err

handleMessage :: FilePath -> WS.Connection -> NumClient -> Chan (NumClient, Message) -> Message -> IO ()
handleMessage msgPath conn nc msChan msg = do
    case payload msg of
        InitiatedConnection connection -> do
            let alluuids = uuids connection
            esevs <- readMessages msgPath
            let msgs = filter (\e -> uuid (metadata e) `notElem` alluuids) esevs
            mapM_ (WS.sendTextData conn . JSON.encode) msgs
            putStrLn $ "\nSent all missing " ++ show (length msgs) ++ " messsages to client " ++ show nc
            -- Send back and store an ACK to let the client know the message has been stored
            -- Except for events that should be handled by another service
            let msg' = setCreator "store" $ setFlow Sent msg
            appendMessage msgPath msg'
            putStrLn $ "\nStored this message: " ++ show msg'
            WS.sendTextData conn $ JSON.encode msg'
            writeChan msChan (nc, msg')
        _ -> do
            -- first store eveything except the connection initiation
            appendMessage msgPath msg
            when (getFlow msg == Requested) $ do
                WS.sendTextData conn $ JSON.encode $ setFlow Sent msg
            putStrLn $ "\nStored this message and broadcast to other microservice threads: " ++ show msg
            -- send the msgs to other connected clients
            writeChan msChan (nc, msg)
            unless (msg `isType` "InitiatedConnection" || getFlow msg == Processed) $ do
                -- Set all messages as processed, except those for Ident or are already processed
                let processedMsg = setFlow Processed msg
                appendMessage msgPath processedMsg
                putStrLn $ "\nStored this message: " ++ show processedMsg
                WS.sendTextData conn $ JSON.encode processedMsg
                writeChan msChan (nc, processedMsg)

-- if the event is a InitiatedConnection, get the uuid list from it,
-- and send back all the missing events (with an added ack)

serve :: Options -> IO ()
serve (Options storePath listHost listenPort) = do
    st <- newMVar 0
    chan <- newChan
    putStrLn $ "Modelyz Store, serving from localhost:" ++ show listenPort ++ "/"
    WS.runServerWithOptions WS.defaultServerOptions{WS.serverHost = listHost, WS.serverPort = listenPort} (wsApp storePath chan st)

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
