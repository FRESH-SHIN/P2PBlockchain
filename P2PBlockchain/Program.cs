using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace P2PBlockchain
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            if (args.Length == 1)
            {
                UDPRelayServer.StartServer();

            }
            else
            {
                KeyPairs keyPairs;
                if (!File.Exists("keypairs.json"))
                {
                    keyPairs = new KeyPairs();
                    Console.WriteLine("Creating key pairs...");
                    string jsonkey = keyPairs.ToJson();
                    Console.WriteLine(jsonkey);
                    File.WriteAllText("keypairs.json", jsonkey);
                }
                else
                {
                    keyPairs = KeyPairs.FromJson(File.ReadAllText("keypairs.json"));
                }
                if (!File.Exists("0.json"))
                {
                    Token token = Token.CreateInitialToken(Encoding.UTF8.GetBytes("This is my first implementation of a blockchain"), keyPairs);
                    TokenContainer tokenContainer = new TokenContainer(token, keyPairs);
                    Console.WriteLine(tokenContainer.ToJson());
                    File.WriteAllText("0.json", tokenContainer.ToJson());
                }
                else
                {
                    Console.WriteLine("Validating Signature");
                    TokenContainer initialTokenContainer = TokenContainer.FromJson(File.ReadAllText("0.json"));
                    Console.WriteLine(initialTokenContainer.VerifyValidToken());
                }
                if (false && !File.Exists("1.json"))
                {
                    Token token = new Token();
                    token.Data = Encoding.UTF8.GetBytes("1234");
                    token.PrevTokenHash = SHA256.HashData((File.ReadAllBytes("0.gson")));
                    token.TokenNumber = 1;
                    token.Timestamp = DateTime.UtcNow.ToBinary();
                    token.NextKeySize = 4;
                    token.PublicKey.Exponent = keyPairs.Exponent;
                    token.PublicKey.Modulus = keyPairs.Modulus;
                    TokenContainer tokenContainer = new TokenContainer(token, keyPairs);
                    Console.WriteLine(tokenContainer.ToJson());
                    File.WriteAllText("1.json", tokenContainer.ToJson());
                }/*
                (UdpClient udpClient, IPEndPoint ipep) = UDPRelayClient.UDPPeer(IPEndPoint.Parse(Config.KnownURL[0]));
                Console.WriteLine("Establish a connection with ({0})", ipep.ToString());
                udpClient.Connect(ipep);
                IPEndPoint iPEndPoint = new IPEndPoint(IPAddress.Any, 0);
                byte[] buffer = udpClient.Receive(ref iPEndPoint);
                Console.WriteLine("Packet from {0}", iPEndPoint.ToString());
                (short s, IPEndPoint selfIped) = Payloads.ParsePayload(buffer);
                Console.WriteLine("Payload : {0} {1} (Self)", s, selfIped.ToString());
                Console.WriteLine("Connection Established");
                TCPSocketOverUDPSocket p = new TCPSocketOverUDPSocket();
                //p.Socket.Bind(ipep);
                //p.Connect(ipep.Address, ipep.Port);
                //p.OnConnectionSuccessful += P_OnConnectionSuccessful;
                //p.OnMessageReceived += P_OnMessageReceived;
                */
                MainThread mainThread = new MainThread(10);
                Thread.Sleep(-1);
            }

        }
    }

    class MainThread
    {
        private ConfigManager configManager;
        private List<PeerManager> peerManagers;
        private Task receivedTask;
        private Task updateTokenTask;
        public MainThread(int peers)
        {
            receivedTask = Task.Factory.StartNew(() => { });
            peerManagers = new List<PeerManager>();
            configManager = new ConfigManager();
            for (int i = 0; i < peers; i++)
            {
                PeerManager p = new PeerManager(configManager);
                peerManagers.Add(p);
                p.p.OnMessageReceived += P_OnMessageReceived; //???????????
            }
            updateTokenTask = Task.Factory.StartNew(() => { });
            updateTokenTask.ContinueWith((task) => { RequestLastTokenIndex(task); });

        }

        private void P_OnMessageReceived(object sender, MessageReceivedEventArgs e)
        {
            receivedTask.ContinueWith((task) =>
            {
                byte[] buffer = new byte[e.Data.Length];
                e.Data.CopyTo(buffer, 0);
                Received(sender, buffer);
            });
        }
        private void Received(object sender, byte[] buffer)
        {
            int flag = BitConverter.ToInt32(buffer, 0);
            if (flag == 0)
            {
                int latestIndex = BitConverter.ToInt32(buffer, 4);
                Console.WriteLine("Flag {0} UpdateLatestIndex({1})", flag, latestIndex);
                configManager.UpdateLatestIndex(latestIndex);
            }
            if (flag == 1)
            {
                int requestedIndex = BitConverter.ToInt32(buffer, 4);
                Console.WriteLine("Flag {0} RequestedIndex({1})", flag, requestedIndex);
                int a = configManager.GetLocalIndex();
                if (requestedIndex <= a)
                {
                    receivedTask.ContinueWith((task) => { SendTokenInfo(task, a); });
                }
            }
            if (flag == 2)
            {
                int index = BitConverter.ToInt32(buffer, 4);
                Console.WriteLine("Flag {0} TokenInfo({1})", flag, index);
                if (index > configManager.GetLocalIndex())
                {
                    byte[] hash = new byte[32];
                    Array.Copy(buffer, 10, hash, 0, 32);
                    configManager.UpdateTokenInfo(index, BitConverter.ToInt16(buffer, 8), hash);
                }
            }
            if (flag == 3)
            {
                int index = BitConverter.ToInt32(buffer, 4);
                short startIndex = BitConverter.ToInt16(buffer, 8);
                short length = BitConverter.ToInt16(buffer, 10);
                Console.WriteLine("Flag {0} TokenFrag(Index : {1}, FileIndex : {2}, Length : {3})", flag, index, startIndex, length);
                receivedTask.ContinueWith((task) => { SendFragment(task, index, startIndex, length); });
            }
            if (flag == 4)
            {
                int index = BitConverter.ToInt32(buffer, 4);
                short startIndex = BitConverter.ToInt16(buffer, 8);
                short length = BitConverter.ToInt16(buffer, 10);
                byte[] fragment = new byte[length];
                Console.WriteLine("Flag {0} TokenFrag(Index : {1}, FileIndex : {2}, Length : {3})", flag, index, startIndex, length);
                Array.Copy(buffer, 12, fragment, 0, length);
                configManager.UpdateFragment(index, startIndex, length, fragment);
            }
        }
        private void SendFragment(Task task, int i, short startindex, short length)
        {
            byte[] buffer = new byte[1024];
            (short size, _) = configManager.GetTokenInfo(i);
            if (size != 0)
            {
                BitConverter.GetBytes(4).CopyTo(buffer, 0);
                BitConverter.GetBytes(i).CopyTo(buffer, 4);
                BitConverter.GetBytes(startindex).CopyTo(buffer, 8);
                BitConverter.GetBytes(length).CopyTo(buffer, 10);
                configManager.GetFragment(i, startindex, length).CopyTo(buffer, 12);
                Broadcast(buffer);
            }
        }
        private void RequestToken(Task task, int i)
        {
            byte[] buffer = new byte[1024];
            (short size, _) = configManager.GetTokenInfo(i);
            if (size != 0)
            {
                BitConverter.GetBytes(3).CopyTo(buffer, 0);
                BitConverter.GetBytes(i).CopyTo(buffer, 4);
                for (int a = 0; a <= (size / 1000); a++)
                {
                    BitConverter.GetBytes((short)a * 1000).CopyTo(buffer, 8);
                    BitConverter.GetBytes((short)((size - a * 1000) < 1000 ? size % 1000 : 1000)).CopyTo(buffer, 10);
                    Broadcast(buffer);
                }
            }

            task.ContinueWith((r) => { RequestLastTokenIndex(r); });
        }
        private void SendTokenInfo(Task task, int i)
        {
            byte[] buffer = new byte[1024];
            byte[] info = configManager.GetTokenInfoBytes(i);
            if (info != null)
            {
                BitConverter.GetBytes(2).CopyTo(buffer, 0);
                BitConverter.GetBytes(i).CopyTo(buffer, 4);
                info.CopyTo(buffer, 8);
                Broadcast(buffer);
            }

        }
        private void RequestTokenInfo(Task task)
        {
            int localIdx = configManager.GetLocalIndex();
            int latestIdx = configManager.GetLatestIndex();
            if (localIdx < latestIdx)
            {
                int i = localIdx + 1;
                byte[] buffer = new byte[1024];
                BitConverter.GetBytes(1).CopyTo(buffer, 0);
                BitConverter.GetBytes(i).CopyTo(buffer, 4);
                Broadcast(buffer);
                task.ContinueWith((r) => { RequestToken(r, i); });
            }
            else

                task.ContinueWith((r) => { RequestLastTokenIndex(r); });
        }
        private void RequestLastTokenIndex(Task task)
        {
            Thread.Sleep(Config.TokenIndexUpdateInterval * 1000);
            byte[] buffer = new byte[1024];
            BitConverter.GetBytes(0).CopyTo(buffer, 0);
            int index = configManager.GetLocalIndex();
            BitConverter.GetBytes(index).CopyTo(buffer, 4);
            Console.WriteLine("Send Flag {0} UpdateLatestIndex({1})", 0, index);
            Broadcast(buffer);
            task.ContinueWith((r) => { RequestTokenInfo(r); });
        }
        private void Broadcast(byte[] buffer)
        {
            byte[] b = new byte[buffer.Length];
            buffer.CopyTo(b, 0);
            foreach (var item in peerManagers)
            {
                item.SendBytes(b);
            }
        }
    }
    class PeerManager
    {
        private Task task;
        private ConfigManager configManager;
        private TCPSocketOverUDPSocket s;
        private UdpClient udpClient;
        private IPEndPoint ipep;
        public TCPSocketOverUDPSocket p;
        private UDPRelayClient relayClient;
        public PeerManager(ConfigManager cf)
        {
            relayClient = new UDPRelayClient();
            p = new TCPSocketOverUDPSocket();
            p.OnSocketClosed += P_OnSocketClosed;
            p.OnConnectionSuccessful += P_OnConnectionSuccessful;
            configManager = cf;

            task = Task.Factory.StartNew(() => { }, TaskCreationOptions.RunContinuationsAsynchronously);
            task.ContinueWith((a) => { Connect(); });

        }
        public void SendBytes(byte[] buffer)
        {
            if (p.Socket.Connected)
                p.Send(buffer, 0);
        }
        private void Connect()
        {
            udpClient = new UdpClient();
            (ipep) = relayClient.UDPPeer(udpClient, IPEndPoint.Parse(Config.KnownURL[0]), configManager.UID);
            if (ipep == null)
            {
                task.ContinueWith((a) => { Reconnect(Config.PeerRefresh); });
                return;
            }
            try
            {
                Console.WriteLine("Establish a connection with ({0})", ipep.ToString());
                IPEndPoint iPEndPoint = new IPEndPoint(IPAddress.Any, 0);
                udpClient.Send(Payloads.UDPHolePunching(ipep, configManager.UID), 16, ipep);
                var asyncResult = udpClient.BeginReceive(null, null);
                asyncResult.AsyncWaitHandle.WaitOne(Config.RelayServerTimeout * 1000);
                if (!asyncResult.IsCompleted)
                {
                    throw new Exception();
                }
                byte[] buffer = udpClient.EndReceive(asyncResult, ref iPEndPoint);
                Console.WriteLine("Packet from {0}", iPEndPoint.ToString());
                (short s, IPEndPoint selfIped) = Payloads.ParsePayload(buffer);
                Console.WriteLine("Payload : {0} {1} (Self)", s, selfIped.ToString());
                Console.WriteLine("Connection Established");
                p.Socket.Bind(selfIped);
                p.Connect(iPEndPoint.Address, iPEndPoint.Port);
            }
            catch (Exception)
            {
                task.ContinueWith((a) => { Reconnect(Config.PeerRefresh); });
                return;
            }

            task.ContinueWith((a) => { CheckTask(); });
            //p.OnConnectionSuccessful += P_OnConnectionSuccessful;
            //p.OnMessageReceived += P_OnMessageReceived;

        }
        private void P_OnMessageReceived(object sender, MessageReceivedEventArgs e)
        {
            Console.WriteLine("Received");
        }

        private void P_OnConnectionSuccessful(object sender, ConnectionAcceptedEventArgs e)
        {
            //SendBytes(new byte[1024]);
        }

        private void P_OnSocketClosed(object sender, SocketClosedEventArgs e)
        {
            task.ContinueWith((a) => { Reconnect(Config.PeerRefresh); });
        }
        private void CheckTask()
        {
            task.ContinueWith((a) => { Reconnect(Config.PeerCommunicationTimeout); });
        }
        private void Reconnect(int wait)
        {
            Thread.Sleep(wait * 1000);
            if (!p.Socket.Connected)
            {
                p.Socket.Dispose();
                if (udpClient != null)
                {
                    udpClient.Dispose();
                }
                p.ReInitSocket();
                task.ContinueWith((a) => { Connect(); });
            }
            else
            {
                p.Socket.Dispose();
                if (udpClient != null)
                {
                    udpClient.Dispose();
                }
                p.ReInitSocket();
                task.ContinueWith((a) => { CheckTask(); });
            }
        }
    }
    class ConfigManager
    {
        private Dictionary<int, TokenInfo> chain;
        private int remoteindex;
        private int localIndex = 0;
        private Random r;
        public long UID { get; private set; }
        private Task taskManager;
        public ConfigManager()
        {
            chain = new Dictionary<int, TokenInfo>();
            r = new Random();
            UID = (long)r.Next() << 32 | (long)r.Next();

            taskManager = Task.Factory.StartNew(() => { }, TaskCreationOptions.RunContinuationsAsynchronously);

            for (int i = 0; ; i++)
            {
                byte[] gson;
                if (File.Exists(string.Format("{0}.gson", i)))
                {
                    gson = File.ReadAllBytes(string.Format("{0}.gson", i));
                }
                else
                {
                    if (File.Exists(string.Format("{0}.json", i)))
                    {

                        using (FileStream originalFileStream = File.Open(string.Format("{0}.json", i), FileMode.Open))
                        {
                            using (MemoryStream compressedStream = new MemoryStream())
                            {
                                using (var compressor = new GZipStream(compressedStream, CompressionMode.Compress))
                                {
                                    originalFileStream.CopyTo(compressor);
                                }
                                gson = compressedStream.ToArray();
                            }

                        }




                        File.WriteAllBytes(string.Format("{0}.gson", i), gson);
                    }
                    else
                    {
                        break;
                    }
                }
                byte[] hash = SHA256.Create().ComputeHash(gson);
                UpdateTokenInfo(i, (short)gson.Length, hash, gson, true);
                localIndex = i;
            }

        }
        public void InvokeActionWithWait(Action<Task> action)
        {
            taskManager.ContinueWith(action, TaskContinuationOptions.OnlyOnRanToCompletion);
            taskManager.Wait();
        }
        public void InvokeAction(Action<Task> action)
        {
            taskManager.ContinueWith(action, TaskContinuationOptions.OnlyOnRanToCompletion);
        }
        public void UpdateLatestIndex(int idx)
        {
            taskManager.ContinueWith((task) => { if (idx > remoteindex) remoteindex = idx; }, TaskContinuationOptions.OnlyOnRanToCompletion);
        }
        public void UpdateFragment(int idx, short startIdx, short length, byte[] buf)
        {
            Task<bool> t = taskManager.ContinueWith<bool>((task) =>
            {
                if (chain.ContainsKey(idx))
                {
                    return chain[idx].UpdateFrag(startIdx, length, buf);
                }
                return false;
            }, TaskContinuationOptions.OnlyOnRanToCompletion);
            t.Wait();
            if (t.Result)
            {
                localIndex = idx;
            }
        }
        public byte[] GetFragment(int idx, short startIdx, short length)
        {
            byte[] buf = new byte[length];
            Task<byte[]> vs = taskManager.ContinueWith<byte[]>((task) =>
            {
                if (chain.ContainsKey(idx))
                {
                    Array.Copy(chain[idx].gson, startIdx * 1000, buf, 0, length);
                    return buf;
                }
                return null;
            }, TaskContinuationOptions.OnlyOnRanToCompletion);
            vs.Wait();
            return vs.Result;
        }
        public void UpdateTokenInfo(int index, short size, byte[] hash, byte[] gbuf = null, bool isValidated = false)
        {
            taskManager.ContinueWith((task) =>
            {
                if (!chain.ContainsKey(index))
                {
                    chain.Add(index, new TokenInfo(index, size, hash, gbuf, isValidated));
                }
                else if (!chain[index].isValidated)
                {
                    TokenInfo info = chain[index];
                    info.size = size;
                    hash.CopyTo(info.hash, 0);
                    info.gson = new byte[size];
                    if (gbuf != null) gbuf.CopyTo(info.gson, 0);
                }
            }, TaskContinuationOptions.OnlyOnRanToCompletion
            );
        }
        public int GetLocalIndex()
        {
            Task<int> t = taskManager.ContinueWith<int>((task) => { return localIndex; }, TaskContinuationOptions.OnlyOnRanToCompletion);
            t.Wait();
            return t.Result;
        }
        public int GetLatestIndex()
        {
            Task<int> t = taskManager.ContinueWith<int>((task) => { return remoteindex; }, TaskContinuationOptions.OnlyOnRanToCompletion);
            t.Wait();
            return t.Result;
        }
        public (short size, byte[] hash) GetTokenInfo(int i)
        {
            byte[] buffer = new byte[32];
            Task<(short size, byte[] hash)> t = taskManager.ContinueWith<(short size, byte[] hash)>((task) =>
            {
                if (!chain.ContainsKey(i)) return (0, null);
                chain[i].hash.CopyTo(buffer, 0);
                return (chain[i].size, buffer);
            }, TaskContinuationOptions.OnlyOnRanToCompletion);
            t.Wait();
            return t.Result;
        }
        public byte[] GetTokenInfoBytes(int i)
        {
            Task<byte[]> t = taskManager.ContinueWith<byte[]>((task) =>
            {

                if (chain.ContainsKey(i) && chain[i].isValidated)
                {
                    byte[] hash = SHA256.Create().ComputeHash(chain[i].gson);
                    byte[] size = BitConverter.GetBytes((short)chain[i].gson.Length);
                    byte[] payload = new byte[size.Length + hash.Length];
                    size.CopyTo(payload, 0);
                    hash.CopyTo(payload, size.Length);
                    return payload;
                }
                else
                {
                    return null;
                }
            }, TaskContinuationOptions.OnlyOnRanToCompletion);
            t.Wait();
            return t.Result;
        }
    }
    class TokenInfo
    {
        public TokenContainer container;
        public int index;
        public short size;
        public byte[] hash;
        public byte[] gson;
        private bool[] isReceived;
        public bool isValidated { get; private set; }
        public TokenInfo(int index, short size, byte[] hash, byte[] gbuf = null, bool isValidated = false)
        {
            this.index = index;
            isReceived = new bool[size / 1000 + 1];
            if (gbuf != null) container = TokenContainer.FromGson(gbuf);
            this.hash = new byte[hash.Length];
            hash.CopyTo(hash, 0);
            this.size = size;
            this.isValidated = isValidated;
            gson = new byte[size];
            if (gbuf != null) gbuf.CopyTo(gson, 0);
        }
        public bool UpdateFrag(short startIdx, short length, byte[] buf)
        {
            if (!isValidated)
            {
                isReceived[startIdx / 1000] = true;
                buf.CopyTo(gson, startIdx);
                for (int i = 0; i < isReceived.Length; i++)
                {
                    if (isReceived[i] == false) break;
                    if (i == isReceived.Length - 1)
                    {
                        byte[] receivedHash = SHA256.HashData(gson);
                        for (int j = 0; j < 32; j++)
                        {
                            if (receivedHash[i] != hash[i])
                            {
                                return false;
                            }
                        }
                        container = TokenContainer.FromGson(gson);
                        isValidated = container.VerifyValidToken();
                        if (isValidated)
                        {
                            File.WriteAllBytes(string.Format("{0}.gson", index), gson);
                            using (MemoryStream compressedStream = new MemoryStream(gson))
                            {

                                using (MemoryStream decompressStream = new MemoryStream())
                                {
                                    using (var decompressor = new GZipStream(compressedStream, CompressionMode.Decompress))
                                    {
                                        decompressor.CopyTo(decompressStream);
                                    }
                                    File.WriteAllBytes(string.Format("{0}.json", index), decompressStream.ToArray());
                                }
                            }
                        }
                    }

                }
            }
            return isValidated;

        }

    }

    class TCPSocketOverUDPSocket
    {
        // https://github.com/jasonpang/tcp-holepunching
        public Socket Socket { get; private set; }
        public byte[] Buffer { get; set; }

        /// <summary>
        /// Occurs after an accepted client has been registered.
        /// </summary>
        public event EventHandler<ConnectionAcceptedEventArgs> OnConnectionSuccessful;
        public event EventHandler<MessageSentEventArgs> OnMessageSent;
        public event EventHandler<MessageReceivedEventArgs> OnMessageReceived;
        public event EventHandler<SocketClosedEventArgs> OnSocketClosed;

        public TCPSocketOverUDPSocket()
        {
            Socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            // Set our special Tcp hole punching socket options
            Socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            Socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
            Buffer = new byte[1024];
        }
        /// <summary>
        /// </summary>
        public void Connect(IPAddress host, int port)
        {
            Task_BeginConnecting(host, port);
        }
        public void ReInitSocket()
        {
            Socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            // Set our special Tcp hole punching socket options
            Socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            Socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        }
        private void Task_BeginConnecting(IPAddress host, int port)
        {
            var task = Task.Factory.FromAsync(
                Socket.BeginConnect(host, port, null, null), Socket.EndConnect);
            task.ContinueWith(nextTask => Task_OnConnectSuccessful(), TaskContinuationOptions.OnlyOnRanToCompletion);
        }

        private void Task_OnConnectSuccessful()
        {
            Console.WriteLine(String.Format("Connected to {0}.", Socket.RemoteEndPoint));

            Task_BeginReceive();

            // Invoke the event
            if (OnConnectionSuccessful != null)
                OnConnectionSuccessful(this, new ConnectionAcceptedEventArgs() { Socket = Socket });
        }

        public void Send(byte[] data, int MessageType)
        {
            // If the registrant exists
            if (Socket.Connected)
            {
                var task = Task.Factory.FromAsync<Int32>(Socket.BeginSend(data, 0, data.Length, SocketFlags.None, null, Socket), Socket.EndSend);
                task.ContinueWith(nextTask => Task_OnSendCompleted(task.Result, data.Length, Socket.RemoteEndPoint, MessageType), TaskContinuationOptions.OnlyOnRanToCompletion);
            }
        }

        private void Task_OnSendCompleted(int numBytesSent, int expectedBytesSent, EndPoint to, int messageType)
        {
            if (numBytesSent != expectedBytesSent)
                Console.WriteLine(String.Format("Warning: Expected to send {0} bytes but actually sent {1}!",
                                                expectedBytesSent, numBytesSent));

            Console.WriteLine(String.Format("Sent a {0} byte {1}Message to {2}.", numBytesSent, messageType, to));

            if (OnMessageSent != null)
                OnMessageSent(this, new MessageSentEventArgs() { Length = numBytesSent, To = to });
        }

        private void Task_BeginClose()
        {
            ShutdownAndClose();
            if (OnSocketClosed != null)
                OnSocketClosed(this, new SocketClosedEventArgs() { });
        }
        private void Task_BeginReceive()
        {
            Task<Int32> task = null;
            try
            {
                task = Task.Factory.FromAsync<Int32>(Socket.BeginReceive(Buffer, 0, Buffer.Length, SocketFlags.None, null, null), Socket.EndReceive);
            }
            catch (Exception ex)
            {
                var exceptionMessage = (ex.InnerException != null) ? ex.InnerException.Message : ex.Message;
                Console.WriteLine(exceptionMessage);
                Task_BeginClose();
            }
            if (task != null)
                task.ContinueWith(nextTask =>
                {
                    try
                    {
                        Task_OnReceiveCompleted(task.Result);
                        Task_BeginReceive(); // Receive more data
                    }
                    catch (Exception ex)
                    {
                        var exceptionMessage = (ex.InnerException != null) ? ex.InnerException.Message : ex.Message;
                        Console.WriteLine(exceptionMessage);
                        Task_BeginClose();
                    }
                }, TaskContinuationOptions.OnlyOnRanToCompletion);
        }

        private void Task_OnReceiveCompleted(int numBytesRead)
        {
            Console.WriteLine(String.Format("Received a {0} byte {1}Message from {2}.", numBytesRead, 0, Socket.RemoteEndPoint));
            byte[] data = new byte[Buffer.Length];
            Buffer.CopyTo(data, 0);
            if (OnMessageReceived != null)
                OnMessageReceived(this, new MessageReceivedEventArgs() { From = (IPEndPoint)Socket.RemoteEndPoint, Data = data });
        }

        /// <summary>
        /// Only disconnects without shutting down.
        /// </summary>
        public void Disconnect()
        {
            Socket.Disconnect(true);
        }

        /// <summary>
        /// Explicitly stops sending and receiving, and then closes the socket.
        /// </summary>
        public void ShutdownAndClose()
        {
            Console.WriteLine("Shutting down socket...");

            try
            {
                Socket.Shutdown(SocketShutdown.Both);
                Socket.Dispose();
            }
            catch
            {
            }
        }
    }

    internal class ConnectionAcceptedEventArgs
    {
        public ConnectionAcceptedEventArgs()
        {
        }

        public Socket Socket { get; set; }
    }

    internal class MessageReceivedEventArgs
    {
        public MessageReceivedEventArgs()
        {
        }

        public IPEndPoint From { get; set; }
        public byte[] Data { get; set; }
    }

    internal class MessageSentEventArgs
    {
        public MessageSentEventArgs()
        {
        }

        public int Length { get; set; }
        public EndPoint To { get; set; }
    }

    internal class SocketClosedEventArgs
    {
        public SocketClosedEventArgs()
        {
        }
    }

    class PeerCommunicationPayloads
    {
        //0x00 0x02 : Flag
        //0x02 0x04 : Counter
        //0x06 0x04 : Counter2(if flag is response)
        //0x0A 0x04 : IPAddress self
        //0x0E 0x02 : Port
        //0x08 0x04 : Size
        //0x0C 0x0
        public static byte[] InitialPayload =
            new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
    }
    class PeerCommunicationPayloadOptions
    {

    }
    class UDPRelayClient
    {
        public IPEndPoint UDPPeer(UdpClient udp, IPEndPoint iped, long UID)
        {
            //UdpClient udpClient = new UdpClient();
            IPEndPoint sender = new IPEndPoint(IPAddress.Any, 0);
            //udpClient.Connect(iped);
            //udpClient.Send(Payloads.HeartbeatPayload(iped), 8, iped);
            try
            {
                //byte[] buffer = udpClient.Receive(ref sender);
                sender = GetPeer(udp, iped, UID);
                return sender;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return (null);
            }
        }
        public IPEndPoint GetPeer(UdpClient udp, IPEndPoint iped, long UID)
        {
            int i = Config.PeerFindTrial;
            int j = Config.PeerFindTimeout;
            udp.Send(Payloads.HeartbeatPayload(iped, UID), 16, iped);
            while (i-- > 0)
            {
                var asyncResult = udp.BeginReceive(null, null);
                while (j-- > 0)
                {
                    asyncResult.AsyncWaitHandle.WaitOne(1000);
                    if (asyncResult.IsCompleted)
                    {
                        try
                        {
                            IPEndPoint remoteEP = new IPEndPoint(iped.Address, iped.Port);
                            byte[] receivedData = udp.EndReceive(asyncResult, ref remoteEP);
                            (short flag, IPEndPoint ipep) = Payloads.ParsePayload(receivedData);
                            if (flag == PayloadFlagOptions.UDPRelay)
                            {
                                return ipep;
                            }
                            break;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.ToString());
                            break;
                            // EndReceive failed and we ended up here
                        }
                    }
                    else
                    {
                        // The operation wasn't completed before the timeout and we're off the hook
                    }
                }
            }
            return null;


        }
    }
    class UDPRelayServer
    {
        public static long UID;
        public static int DefaultPort = 9473;
        private static SynchronizedCollection<Connection> connections = new SynchronizedCollection<Connection>();
        public static void StartServer()
        {
            Random r = new Random();
            UID = (long)r.Next() << 32 | (long)r.Next();
            SynchronizedCollection<byte[]> reports = new SynchronizedCollection<byte[]>();
            IPEndPoint iped = new IPEndPoint(IPAddress.Any, DefaultPort);
            Console.WriteLine("Waiting for UDP Connection");
            Thread thread = new Thread(() => ConnectionManager());
            thread.Start();
            while (true)
            {
                IPEndPoint sender = new IPEndPoint(IPAddress.Any, 0);
                byte[] buffer = new byte[16];
                UdpClient udpClient = new UdpClient(iped);
                buffer = udpClient.Receive(ref sender);
                udpClient.Send(Payloads.HeartbeatPayload(sender, UID), 16, sender);
                udpClient.Dispose();
                Connection connection = new Connection(sender, DateTime.UtcNow.ToBinary(), BitConverter.ToInt16(buffer, 0), BitConverter.ToInt64(buffer, 8));
                connections.Add(connection);
            }
        }
        public static void ConnectionManager()
        {
            UdpClient udpClient = new UdpClient();
            List<Connection> activeConnections = new List<Connection>();
            while (true)
            {
                while (connections.Count > 0)
                {
                    Connection c = connections[0];
                    connections.RemoveAt(0);
                    int idx = activeConnections.FindIndex(a => a.UID == c.UID && a.endPoint.ToString() == c.endPoint.ToString());
                    if (idx > -1)
                    {
                        if (activeConnections[idx].Established == true)
                        {
                            if (DateTime.UtcNow - DateTime.FromBinary(c.lastPong) > TimeSpan.FromSeconds(Config.PeerListLifetime))
                            {
                                activeConnections.RemoveAt(idx);
                                activeConnections.Add(c);
                            }
                        }
                        else
                        {

                            activeConnections[idx].lastPong = c.lastPong;
                            c = activeConnections[idx];
                            activeConnections.RemoveAt(idx);
                            activeConnections.Add(c);
                        }
                    }
                    else
                    {
                        activeConnections.Add(c);
                    }
                    List<Connection> tempList = new List<Connection>(activeConnections);
                    if (DateTime.UtcNow - DateTime.FromBinary(c.lastPong) < TimeSpan.FromSeconds(Config.PeerFindTimeout))
                    {
                        for (int i = 0; i < activeConnections.Count - 1; i++)
                        {
                            Connection temp = tempList[i];
                            if (DateTime.UtcNow - DateTime.FromBinary(temp.lastPong) < TimeSpan.FromSeconds(Config.PeerFindTimeout))
                            {
                                if (temp.Established == false && c.Established == false)
                                {
                                    if (c.UID != temp.UID)

                                    {
                                        udpClient.Send(Payloads.UDPRelayPayload(c.endPoint, UID), 16, temp.endPoint);
                                        udpClient.Send(Payloads.UDPRelayPayload(temp.endPoint, UID), 16, c.endPoint);
                                        temp.Established = true;
                                        c.Established = true;
                                    }
                                }
                            }
                            else
                            {
                                activeConnections.Remove(temp);
                            }

                        }
                    }
                    else
                    {
                        activeConnections.Remove(c);
                    }
                }
                Thread.Sleep(500);

            }
        }
    }
    class Connection
    {
        public long UID;
        public bool Established = false;
        public IPEndPoint endPoint;
        public long lastPong;
        public short flag;
        public Connection(IPEndPoint endPoint, long pongtime, short flag, long UID)
        {
            this.endPoint = endPoint;
            lastPong = pongtime;
            this.flag = flag;
            this.UID = UID;
        }

    }
    class PayloadFlagOptions
    {
        public static short Heartbeat = 1;
        public static short PingPong = 2;
        public static short UDPRelay = 4;
        public static short UDPHolePunching = 8;
    }
    class Payloads
    {
        public static byte[] HeartbeatPayload(IPEndPoint sender, long UID)
        {
            short f = PayloadFlagOptions.Heartbeat;
            byte[] flag = BitConverter.GetBytes(f);
            byte[] ipaddress = sender.Address.GetAddressBytes();
            byte[] ports = BitConverter.GetBytes((short)sender.Port);
            byte[] u = BitConverter.GetBytes(UID);
            byte[] payload = new byte[] { flag[0], flag[1], ipaddress[0], ipaddress[1], ipaddress[2], ipaddress[3], ports[0], ports[1] };
            byte[] ret = new byte[16];
            payload.CopyTo(ret, 0);
            u.CopyTo(ret, 8);
            return ret;
        }
        public static byte[] PingPongPayloads(IPEndPoint sender, long UID)
        {
            short f = PayloadFlagOptions.PingPong;
            byte[] flag = BitConverter.GetBytes(f);
            byte[] ipaddress = sender.Address.GetAddressBytes();
            byte[] ports = BitConverter.GetBytes((short)sender.Port);
            byte[] u = BitConverter.GetBytes(UID);
            byte[] payload = new byte[] { flag[0], flag[1], ipaddress[0], ipaddress[1], ipaddress[2], ipaddress[3], ports[0], ports[1] };
            byte[] ret = new byte[16];
            payload.CopyTo(ret, 0);
            u.CopyTo(ret, 8);
            return ret;
        }
        public static byte[] UDPRelayPayload(IPEndPoint sender, long UID)
        {
            short f = PayloadFlagOptions.UDPRelay;
            byte[] flag = BitConverter.GetBytes(f);
            byte[] ipaddress = sender.Address.GetAddressBytes();
            byte[] ports = BitConverter.GetBytes((short)sender.Port);
            byte[] u = BitConverter.GetBytes(UID);
            byte[] payload = new byte[] { flag[0], flag[1], ipaddress[0], ipaddress[1], ipaddress[2], ipaddress[3], ports[0], ports[1] };
            byte[] ret = new byte[16];
            payload.CopyTo(ret, 0);
            u.CopyTo(ret, 8);
            return ret;
        }
        public static byte[] UDPHolePunching(IPEndPoint sender, long UID)
        {
            short f = PayloadFlagOptions.UDPHolePunching;
            byte[] flag = BitConverter.GetBytes(f);
            byte[] ipaddress = sender.Address.GetAddressBytes();
            byte[] ports = BitConverter.GetBytes((short)sender.Port);
            byte[] u = BitConverter.GetBytes(UID);
            byte[] payload = new byte[] { flag[0], flag[1], ipaddress[0], ipaddress[1], ipaddress[2], ipaddress[3], ports[0], ports[1] };
            byte[] ret = new byte[16];
            payload.CopyTo(ret, 0);
            u.CopyTo(ret, 8);
            return ret;
        }
        public static (short, IPEndPoint) ParsePayload(byte[] payload)
        {
            short flag = BitConverter.ToInt16(payload, 0);
            IPEndPoint ipep = new IPEndPoint(new IPAddress(new byte[] { payload[2], payload[3], payload[4], payload[5] }), (int)BitConverter.ToUInt16(payload, 6));
            return (flag, ipep);
        }
    }
    class TokenContainer
    {
        static public TokenContainer FromJson(string json)
        {
            return (TokenContainer)JsonSerializer.Deserialize(json, typeof(TokenContainer));
        }
        static public TokenContainer FromGson(byte[] gson)
        {
            using (MemoryStream compressedStream = new MemoryStream(gson))
            {

                using (MemoryStream decompressStream = new MemoryStream())
                {
                    using (var decompressor = new GZipStream(compressedStream, CompressionMode.Decompress))
                    {
                        decompressor.CopyTo(decompressStream);
                    }
                    return (TokenContainer)JsonSerializer.Deserialize(decompressStream.ToArray(), typeof(TokenContainer));
                }

            }


        }
        public TokenContainer()
        {
        }
        public TokenContainer(Token token, KeyPairs keyPairs)
        {
            this.Token = token;
            this.Signature = HashAndSignBytes(Encoding.UTF8.GetBytes(token.ToJson()), keyPairs.RSAParams);

        }
        private static byte[] HashAndSignBytes(byte[] DataToSign, RSAParameters Key)
        {
            try
            {
                // Create a new instance of RSACryptoServiceProvider using the
                // key from RSAParameters.
                RSACryptoServiceProvider RSAalg = new RSACryptoServiceProvider();

                RSAalg.ImportParameters(Key);

                // Hash and sign the data. Pass a new instance of SHA256
                // to specify the hashing algorithm.
                return RSAalg.SignData(DataToSign, SHA256.Create());
            }
            catch (CryptographicException e)
            {
                Console.WriteLine(e.Message);

                return null;
            }
        }

        private static bool VerifySignedHash(byte[] DataToVerify, byte[] SignedData, RSAParameters Key)
        {
            try
            {
                // Create a new instance of RSACryptoServiceProvider using the
                // key from RSAParameters.
                RSACryptoServiceProvider RSAalg = new RSACryptoServiceProvider();

                RSAalg.ImportParameters(Key);

                // Verify the data using the signature.  Pass a new instance of SHA256
                // to specify the hashing algorithm.
                return RSAalg.VerifyData(DataToVerify, SHA256.Create(), SignedData);
            }
            catch (CryptographicException e)
            {
                Console.WriteLine(e.Message);

                return false;
            }
        }

        public bool VerifySign()
        {
            return VerifySignedHash(Encoding.UTF8.GetBytes(this.Token.ToJson()), this.Signature, this.Token.PublicKey.RSAParams);
        }
        public bool VerifyValidToken()
        {
            if (Token.Data.Length <= Config.DataSize)
            {
                return VerifySign();
            }
            return false;
        }
        public string ToJson()
        {
            return JsonSerializer.Serialize(this, typeof(TokenContainer));
        }
        public Token Token { get; set; } = new Token();
        public byte[] Signature
        {
            get; set;
        }

    }
    class Token
    {
        public string ToJson()
        {
            return JsonSerializer.Serialize(this, typeof(Token));
        }
        public static Token GetTokenFromFile(string path)
        {
            if (File.Exists(path))
            {
                using MemoryStream memoryStream = new MemoryStream();
                using FileStream compressedFileStream = File.Open(path, FileMode.Open);
                using var decompressor = new GZipStream(compressedFileStream, CompressionMode.Decompress);
                decompressor.CopyTo(memoryStream);
                return (Token)JsonSerializer.Deserialize(memoryStream.ToArray(), typeof(Token));
            }
            else
            {
                throw new FileNotFoundException();
            }
        }
        public static Token CreateInitialToken(byte[] data, KeyPairs key)
        {
            Token token = new Token();
            token.Data = new byte[data.Length];
            data.CopyTo(token.Data, 0);
            token.PrevTokenHash = ((MD5.Create()).ComputeHash(Encoding.UTF8.GetBytes(Config.Name)));
            token.TokenNumber = 0;
            token.Timestamp = DateTime.UtcNow.ToBinary();
            token.NextKeySize = 4;
            token.PublicKey.Exponent = key.Exponent;
            token.PublicKey.Modulus = key.Modulus;
            return token;
        }

        public byte[] Identifier
        {
            get
            {
                return ((MD5.Create()).ComputeHash(Encoding.UTF8.GetBytes(Config.Name)));
            }
        }
        public int Version
        {
            get
            {
                return Config.Version;
            }
        }
        public int TokenNumber
        {
            get; set;
        }
        public byte[] PrevTokenHash { get; set; }
        public long Timestamp
        {
            get; set;
        }
        public byte[] Data
        {
            get; set;
        }
        public int NextKeySize
        {
            get; set;
        }
        public PublicKey PublicKey { get; set; } = new PublicKey();
    }
    class PublicKey
    {
        private int keyLength = Config.KeyLength;
        private RSAParameters rsaParams = new RSAParameters();
        [JsonIgnore]
        public RSAParameters RSAParams { get { return rsaParams; } }
        public PublicKey(byte[] Modulus, byte[] Exponent)
        {
            rsaParams.Modulus = Modulus;
            rsaParams.Exponent = Exponent;
        }
        public PublicKey()
        {

        }
        public byte[] Modulus { get { return rsaParams.Modulus; } set { rsaParams.Modulus = value; } }
        public byte[] Exponent { get { return rsaParams.Exponent; } set { rsaParams.Exponent = value; } }
    }
    class KeyPairs
    {
        private int keyLength = Config.KeyLength;
        /// <summary>
        /// Create new key paris
        /// </summary>
        public KeyPairs()
        {
            using (RSACryptoServiceProvider RSA = new RSACryptoServiceProvider(keyLength))
            {
                RSAParams = RSA.ExportParameters(true);
            }
        }
        static public KeyPairs FromJson(string json)
        {
            KeyPairs keyPairs = new KeyPairs();
            keyPairs = JsonSerializer.Deserialize<KeyPairs>(json);
            return keyPairs;
        }

        public string ToJson()
        {
            return JsonSerializer.Serialize(this);
        }
        public RSAParameters RSAParams = new RSAParameters();
        public byte[] D { get { return RSAParams.D; } set { RSAParams.D = value; } }
        public byte[] DP { get { return RSAParams.DP; } set { RSAParams.DP = value; } }
        public byte[] DQ { get { return RSAParams.DQ; } set { RSAParams.DQ = value; } }
        public byte[] Exponent { get { return RSAParams.Exponent; } set { RSAParams.Exponent = value; } }
        public byte[] InverseQ { get { return RSAParams.InverseQ; } set { RSAParams.InverseQ = value; } }
        public byte[] Modulus { get { return RSAParams.Modulus; } set { RSAParams.Modulus = value; } }
        public byte[] P { get { return RSAParams.P; } set { RSAParams.P = value; } }
        public byte[] Q { get { return RSAParams.Q; } set { RSAParams.Q = value; } }
        public int KeyLength { get { return keyLength; } }
    }
    class Config
    {
        public static int PeerRefresh = 5;
        public static string Name = "OHNUX";
        public static int KeyLength = 2048;
        public static int DataSize = 4096;
        public static int Version = 0;
        public static string[] KnownURL = { "127.0.0.1:9473" };
        public static int RelayServerTimeout = 10;
        public static int PeerCommunicationTimeout = 10;
        public static int PeerFindTimeout = 5;
        public static int PeerFindTrial = 10;
        public static int PeerListLifetime = 30;
        public static int TokenIndexUpdateInterval = 5;
        public static int TokenInfoUpdateInterval = 60;
    }

}
