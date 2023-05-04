{-# LANGUAGE OverloadedStrings #-}

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
import Data.Aeson.KeyMap qualified as KeyMap
import Data.List ()
import Data.List qualified as List

-- import Data.Map qualified as Map (Map)
import Data.Text qualified as T
import Message (Message, appendMessage, isProcessed, isType, readMessages, setFlow)
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
                    when (n /= nc && not (isType "InitiatedConnection" ev)) $ do
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
            case JSON.decode
                ( case messages of
                    WS.Text bs _ -> WS.fromLazyByteString bs
                    WS.Binary bs -> WS.fromLazyByteString bs
                ) of
                Just evs -> mapM (handleMessage f conn nc mschan) evs
                Nothing -> sequence [putStrLn "\nError decoding incoming message"]

handleMessage :: FilePath -> WS.Connection -> NumClient -> Chan (NumClient, Message) -> Message -> IO ()
handleMessage f conn nc msChan msg = do
    -- first store eveything except the connection initiation
    unless (isType "InitiatedConnection" msg) $ do
        appendMessage f msg
        WS.sendTextData conn $ JSON.encode $ KeyMap.singleton "messages" $ List.singleton (setFlow "Sent" msg)
        putStrLn $ "\nStored this message and broadcast to other microservice threads: " ++ show msg
        -- send the msgs to other connected clients
        writeChan msChan (nc, msg)
        unless (isType "AddedIdentifier" msg || isProcessed msg) $ do
            -- Set all messages as processed, except those for Ident or are already processed
            let msg' = setFlow "Sent" msg
            appendMessage f msg'
            putStrLn $ "\nStored this message: " ++ show msg'
            WS.sendTextData conn $ JSON.encode [msg']
            writeChan msChan (nc, msg')
    -- if the event is a InitiatedConnection, get the uuid list from it,
    -- and send back all the missing events (with an added ack)
    when (isType "InitiatedConnection" msg) $ do
        let uuids = getUuids msg
        esevs <- readMessages f
        let evs =
                filter
                    ( \e -> case getMetaString "uuid" e of
                        Just u -> T.unpack u `notElem` uuids
                        Nothing -> False
                    )
                    esevs
        WS.sendTextData conn $ JSON.encode evs
        putStrLn $ "\nSent all missing " ++ show (length evs) ++ " messsages to client " ++ show nc
        -- Send back and store an ACK to let the client know the message has been stored
        -- Except for events that should be handled by another service
        let msg' = setFlow "Sent" msg
        appendMessage f msg'
        putStrLn $ "\nStored this message: " ++ show msg'
        WS.sendTextData conn $ JSON.encode [msg']
        writeChan msChan (nc, msg')

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
