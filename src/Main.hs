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
import qualified Data.Aeson as JSON
import Data.List ()
import qualified Data.Text as T
import Message (Message, getMetaString, getUuids, isProcessed, isType, setProcessed)
import qualified MessageStore as ES
import qualified Network.WebSockets as WS
import Options.Applicative

-- port, file
data Options = Options FilePath Host Port

type NumClient = Int
type Host = String
type Port = Int

options :: Parser Options
options =
    Options
        <$> strOption (short 'f' <> long "file" <> value "messagestore.txt" <> help "Filename of the file containing events")
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
handleMessage f conn nc msChan ev = do
    -- first store eveything except the connection initiation
    unless (isType "InitiatedConnection" ev) $ do
        ES.appendMessage f ev
        putStrLn $ "\nStored this message and broadcast to other microservice threads: " ++ show ev
        -- send the msgs to other connected clients
        writeChan msChan (nc, ev)
        unless (isType "AddedIdentifier" ev || (isProcessed ev) $ do
            -- Set all messages as processed, except those for Ident or are already processed
            let ev' = setProcessed ev
            ES.appendMessage f ev'
            putStrLn $ "\nStored this message: " ++ show ev'
            WS.sendTextData conn $ JSON.encode [ev']
            writeChan msChan (nc, ev')
    -- if the event is a InitiatedConnection, get the uuid list from it,
    -- and send back all the missing events (with an added ack)
    when (isType "InitiatedConnection" ev) $ do
        let uuids = getUuids ev
        esevs <- ES.readMessages f
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
        let ev' = setProcessed ev
        ES.appendMessage f ev'
        putStrLn $ "\nStored this message: " ++ show ev'
        WS.sendTextData conn $ JSON.encode [ev']
        writeChan msChan (nc, ev')

serve :: Options -> IO ()
serve (Options f h p) = do
    st <- newMVar 0
    chan <- newChan
    putStrLn $ "Modelyz Store, serving from localhost:" ++ show p ++ "/"
    WS.runServerWithOptions WS.defaultServerOptions{WS.serverHost = h, WS.serverPort = p} (wsApp f chan st)

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
