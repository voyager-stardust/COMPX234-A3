import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;
public class Server
{
    private static final int PORT = 51234;
    private static final Map<String, String> tupleSpace = new ConcurrentHashMap<>();
    private static final Lock lock = new ReentrantLock();
    private static int totalClients = 0;
    private static int totalOperations = 0;
    private static int totalReads = 0;
    private static int totalGets = 0;
    private static int totalPuts = 0;
    private static int totalErrors = 0;
    public static void main(String[] args)
    {
        try (ServerSocket serverSocket = new ServerSocket(PORT))
        {
            System.out.println("Server started on port " + PORT);
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(Server::printStats, 10, 10, TimeUnit.SECONDS);
            while (true)
            {
                Socket clientSocket = serverSocket.accept();
                totalClients++;
                new Thread(new ClientHandler(clientSocket)).start();
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private static void printStats()
    {
        lock.lock();
        try
        {
            int size = tupleSpace.size();
            double avgTupleSize = size==0 ? 0:totalOperations==0 ? 0:(double) getTotalSize()/size;
            System.out.println("Tuple Space Status");
            System.out.println("Number of tuples: "+size);
            System.out.println("Total clients: "+totalClients);
            System.out.println("Total operations: "+totalOperations);
            System.out.println("Total READs: "+totalReads);
            System.out.println("Total GETs: "+totalGets);
            System.out.println("Total PUTs: "+totalPuts);
            System.out.println("Errors: "+totalErrors);
            System.out.println("Average tuple size: "+String.format("%.2f", avgTupleSize));
        }
        finally
        {
            lock.unlock();
        }
    }
    private static int getTotalSize()
    {
        int totalSize=0;
        for (Map.Entry<String, String> entry : tupleSpace.entrySet())
        {
            totalSize+=entry.getKey().length()+entry.getValue().length();
        }
        return totalSize;
    }
    static class ClientHandler implements Runnable
    {
        private Socket socket;
        ClientHandler(Socket socket)
        {
            this.socket = socket;
        }
        public void run()
        {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())))
            {
                String line;
                while ((line = in.readLine()) != null)
                {
                    String[] parts=parseRequest(line);
                    String command = parts[0];
                    String key = parts[1];
                    String value = parts.length>2 ? parts[2]:"";
                    if (parts == null)
                    {
                        out.write("Invalid request\n");
                        out.flush();
                        continue;
                    }
                    switch (command)
                    {
                        case "R":
                            handleRead(out, key);
                            break;
                        case "G":
                            handleGet(out, key);
                            break;
                        case "P":
                            handlePut(out, key, value);
                            break;
                        default:
                            out.write("Invalid command\n");
                            out.flush();
                    }
                }
            }
            catch (IOException e)
            {
                System.out.println("Client disconnected");
            }
        }

        private String[] parseRequest(String line)
        {
            if (line == null || line.length() < 7)
            {
                return null;
            }
            line = line.trim();
            if (line.length() < 4) return null;
            int msgLen;
            try
            {
                msgLen = Integer.parseInt(line.substring(0, 3));
            }
            catch (NumberFormatException e)
            {
                return null;
            }
            if (line.length() != msgLen + 4)
            {
                return null;
            }
            String content = line.substring(4);
            String[] tokens = content.split(" ", 3);
            return tokens;
        }

        private void handleRead(BufferedWriter out, String key) throws IOException
        {
            lock.lock();
            try
            {
                if (tupleSpace.containsKey(key))
                {
                    String val = tupleSpace.get(key);
                    totalOperations++;
                    totalReads++;
                    out.write("018 OK (" + key + ", " + val + ") read\n");
                }
                else
                {
                    totalOperations++;
                    totalErrors++;
                    out.write("024 ERR " + key + " does not exist\n");
                }
                out.flush();
            }
            finally
            {
                lock.unlock();
            }
        }

        private void handleGet(BufferedWriter out, String key) throws IOException
        {
            lock.lock();
            try
            {
                if (tupleSpace.containsKey(key))
                {
                    String val = tupleSpace.remove(key);
                    totalOperations++;
                    totalGets++;
                    out.write("022 OK ("+key+", "+val+") removed\n");
                }
                else
                {
                    totalOperations++;
                    totalErrors++;
                    out.write("024 ERR "+key+" does not exist\n");
                }
                out.flush();
            }
            finally
            {
                lock.unlock();
            }
        }

        private void handlePut(BufferedWriter out, String key, String value) throws IOException
        {
            lock.lock();
            try
            {
                if (tupleSpace.containsKey(key))
                {
                    totalOperations++;
                    totalErrors++;
                    out.write("024 ERR "+key+" already exists\n");
                }
                else
                {
                    tupleSpace.put(key,value);
                    totalOperations++;
                    totalPuts++;
                    out.write("014 OK ("+key+", "+value+") added\n");
                }
                out.flush();
            }
            finally
            {
                lock.unlock();
            }
        }
    }
}
