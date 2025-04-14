package ch28_socket;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

// Fig. 28.10: ClientTest.java
// Class that tests the Client.
public class Client2 {

    public static void main(String[] args) {
        Client20 application = new Client20(); // create client
        application.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        application.waitForPackets(); // run client application
    }

}


// Fig. 28.9: Client.java
// Client side of connectionless client/server computing with datagrams.
class Client20 extends JFrame {
    private JTextField enterField; // for entering messages
    private JTextArea displayArea; // for displaying messages
    private DatagramSocket socket; // socket to connect to server

    // set up GUI and DatagramSocket
    public Client20() {
        super("Client");

        enterField = new JTextField("Type message here");
        enterField.addActionListener(
                event -> {
                    try // create and send packet
                    {
                        // get message from textfield
                        String message = event.getActionCommand();
                        displayArea.append("\nSending packet containing: " + message + "\n");

                        byte[] data = message.getBytes(); // convert to bytes

                        // create sendPacket
                        DatagramPacket sendPacket = new DatagramPacket(data,
                                data.length, InetAddress.getLocalHost(), 5000);

                        socket.send(sendPacket); // send packet
                        displayArea.append("Packet sent\n");
                        displayArea.setCaretPosition(displayArea.getText().length());
                    } catch (IOException ioException) {
                        displayMessage(ioException + "\n");
                        ioException.printStackTrace();
                    }
                }
        );

        add(enterField, BorderLayout.NORTH);

        displayArea = new JTextArea();
        add(new JScrollPane(displayArea), BorderLayout.CENTER);

        setSize(400, 300); // set window size
        setVisible(true); // show window

        // create DatagramSocket for sending and receiving packets
        try {
            socket = new DatagramSocket();
        } catch (SocketException socketException) {
            socketException.printStackTrace();
            System.exit(1);
        }
    }

    // wait for packets to arrive from Server, display packet contents
    public void waitForPackets() {
        while (true) {
            // receive packet and display contents
            try {
                byte[] data = new byte[100]; // set up packet
                DatagramPacket receivePacket = new DatagramPacket(data, data.length);

                socket.receive(receivePacket); // wait for packet

                // display packet contents
                displayMessage("\nPacket received:" +
                        "\nFrom host: " + receivePacket.getAddress() +
                        "\nHost port: " + receivePacket.getPort() +
                        "\nLength: " + receivePacket.getLength() +
                        "\nContaining:\n\t" + new String(receivePacket.getData(), 0, receivePacket.getLength()));
            } catch (IOException exception) {
                displayMessage(exception + "\n");
                exception.printStackTrace();
            }
        }
    }

    // manipulates displayArea in the event-dispatch thread
    private void displayMessage(final String messageToDisplay) {
        // updates displayArea
        SwingUtilities.invokeLater(
                () -> displayArea.append(messageToDisplay)
        );
    }

}
