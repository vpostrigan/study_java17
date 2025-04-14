package ch28_socket;

import javax.swing.*;
import java.awt.*;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

// Fig. 28.4: ServerTest.java
// Test the Server application.
public class Server1 {

    public static void main(String[] args) {
        Server application = new Server(); // create server
        application.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        application.runServer(); // run server application
    }

}

class Server extends JFrame {
    private JTextField enterField; // inputs message from user
    private JTextArea displayArea; // display information to user
    private ObjectOutputStream output; // output stream to client
    private ObjectInputStream input; // input stream from client
    private ServerSocket server; // server socket
    private Socket connection; // connection to client
    private int counter = 1; // counter of number of connections

    // set up GUI
    public Server() {
        super("Server");

        enterField = new JTextField(); // create enterField
        enterField.setEditable(false);
        // send message to client
        enterField.addActionListener(
                event -> {
                    sendData(event.getActionCommand());
                    enterField.setText("");
                }
        );

        add(enterField, BorderLayout.NORTH);

        displayArea = new JTextArea(); // create displayArea
        add(new JScrollPane(displayArea), BorderLayout.CENTER);

        setSize(300, 150); // set size of window
        setVisible(true); // show window
    }

    // set up and run server
    public void runServer() {
        // set up server to receive connections; process connections
        try {
            server = new ServerSocket(12345, 100); // create ServerSocket

            while (true) {
                try {
                    waitForConnection(); // wait for a connection
                    getStreams(); // get input & output streams
                    processConnection(); // process connection
                } catch (EOFException eofException) {
                    displayMessage("\nServer terminated connection");
                } finally {
                    closeConnection(); //  close connection
                    ++counter;
                }
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    // wait for connection to arrive, then display connection info
    private void waitForConnection() throws IOException {
        displayMessage("Waiting for connection\n");
        connection = server.accept(); // allow server to accept connection
        displayMessage("Connection " + counter + " received from: " +
                connection.getInetAddress().getHostName());
    }

    // get streams to send and receive data
    private void getStreams() throws IOException {
        // set up output stream for objects
        output = new ObjectOutputStream(connection.getOutputStream());
        output.flush(); // flush output buffer to send header information

        // set up input stream for objects
        input = new ObjectInputStream(connection.getInputStream());

        displayMessage("\nGot I/O streams\n");
    }

    // process connection with client
    private void processConnection() throws IOException {
        String message = "Connection successful";
        sendData(message); // send connection successful message

        // enable enterField so server user can send messages
        setTextFieldEditable(true);

        // process messages sent from client
        do {
            // read message and display it
            try {
                message = (String) input.readObject(); // read new message
                displayMessage("\n" + message); // display message
            } catch (ClassNotFoundException classNotFoundException) {
                displayMessage("\nUnknown object type received");
            }
        } while (!message.equals("CLIENT>>> TERMINATE"));
    }

    // close streams and socket
    private void closeConnection() {
        displayMessage("\nTerminating connection\n");
        setTextFieldEditable(false); // disable enterField

        try {
            output.close(); // close output stream
            input.close(); // close input stream
            connection.close(); // close socket
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    // send message to client
    private void sendData(String message) {
        // send object to client
        try {
            output.writeObject("SERVER>>> " + message);
            output.flush(); // flush output to client
            displayMessage("\nSERVER>>> " + message);
        } catch (IOException ioException) {
            displayArea.append("\nError writing object");
        }
    }

    // manipulates displayArea in the event-dispatch thread
    private void displayMessage(final String messageToDisplay) {
        // updates displayArea
        SwingUtilities.invokeLater(
                () -> {
                    displayArea.append(messageToDisplay); // append message
                }
        );
    }

    // manipulates enterField in the event-dispatch thread
    private void setTextFieldEditable(final boolean editable) {
        // sets enterField's editability
        SwingUtilities.invokeLater(
                () -> enterField.setEditable(editable)
        );
    }

}
