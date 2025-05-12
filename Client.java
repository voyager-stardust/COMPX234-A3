import java.io.*;
import java.io.IOException;
import java.net.*;
public class Client
{
    private String hostname;
    private int port;
    private String filename;
    public Client(String hostname, int port, String filename)
    {
        this.hostname = hostname;
        this.port = port;
        this.filename = filename;
    }

    public void run()
    {
        try (Socket socket = new Socket(hostname, port);
             BufferedReader fileReader = new BufferedReader(new FileReader(filename));
             BufferedWriter socketWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
             BufferedReader socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream())))
        {
            String requestLine;
            Socket socket = new Socket(hostname, port);
            socket.setSoTimeout(5000);
            while ((requestLine = fileReader.readLine()) != null)
            {
                String request = buildRequest(requestLine);
                socketWriter.write(request);
                socketWriter.flush();
                String response = socketReader.readLine();
                System.out.println("Request: " + requestLine);
                System.out.println("Response: " + response);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private String buildRequest(String line) throws IOException
    {
        if (line == null || line.trim().isEmpty())
        {
            throw new IOException();
        }
        String[] parts = line.trim().split(" ", 3);
        if (parts.length < 2)
        {
            throw new IOException("error");
        }
        String[] parts = line.trim().split(" ", 3);
        String command = parts[0];
        String key = parts[1];
        String value = parts.length>2 ? parts[2]:"";
        String content;
        if (command.equals("PUT"))
        {
            content = command+" "+key+" "+value;
        }
        else
        {
            content = command+" "+key;
        }
        if (key.length() > 999 || (command.equals("PUT") && value.length() > 999))
        {
            throw new IOException("key is too long");
        }
        int size = content.length();
        String sizeStr = String.format("%03d",size);
        return sizeStr+" "+content+"\n";
    }

    public static void main(String[] args)
    {
        if (args.length!=3)
        {
            System.out.println("Usage: java Client <hostname> <port> <requestFile>");
            return;
        }
        String hostname = args[0];
        int port = Integer.parseInt(args[1]);
        String filename = args[2];
        new Client(hostname, port, filename).run();
    }
}
