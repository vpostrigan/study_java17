package ch28_socket;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

// Fig. 28.8: ServerTest.java
// Class that tests the Server.
public class Server2 {

    public static void main(String[] args) {
        Server20 application = new Server20(); // create server
        application.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        application.waitForPackets(); // run server application
    }

}


// Fig. 28.7: Server.java
// Server side of connectionless client/server computing with datagrams.
class Server20 extends JFrame {
    private JTextArea displayArea; // displays packets received
    private DatagramSocket socket; // socket to connect to client

    // set up GUI and DatagramSocket
    public Server20() {
        super("Server");

        displayArea = new JTextArea(); // create displayArea
        add(new JScrollPane(displayArea), BorderLayout.CENTER);
        setSize(400, 300); // set size of window
        setVisible(true); // show window

        // create DatagramSocket for sending and receiving packets
        try {
            socket = new DatagramSocket(5000);
        } catch (SocketException socketException) {
            socketException.printStackTrace();
            System.exit(1);
        }
    }

    // wait for packets to arrive, display data and echo packet to client
    public void waitForPackets() {
        while (true) {
            // receive packet, display contents, return copy to client
            try {
                byte[] data = new byte[100]; // set up packet
                DatagramPacket receivePacket = new DatagramPacket(data, data.length);

                socket.receive(receivePacket); // wait to receive packet

                // display information from received packet
                displayMessage("\nPacket received:" +
                        "\nFrom host: " + receivePacket.getAddress() +
                        "\nHost port: " + receivePacket.getPort() +
                        "\nLength: " + receivePacket.getLength() +
                        "\nContaining:\n\t" + new String(receivePacket.getData(),
                        0, receivePacket.getLength()));

                sendPacketToClient(receivePacket); // send packet to client
            } catch (IOException ioException) {
                displayMessage(ioException + "\n");
                ioException.printStackTrace();
            }
        }
    }

    // echo packet to client
    private void sendPacketToClient(DatagramPacket receivePacket) throws IOException {
        displayMessage("\n\nEcho data to client...");

        // create packet to send
        DatagramPacket sendPacket = new DatagramPacket(
                receivePacket.getData(), receivePacket.getLength(),
                receivePacket.getAddress(), receivePacket.getPort());

        socket.send(sendPacket); // send packet to client
        displayMessage("Packet sent\n");
    }

    // manipulates displayArea in the event-dispatch thread
    private void displayMessage(final String messageToDisplay) {
        // updates displayArea
        SwingUtilities.invokeLater(
                () -> {
                    displayArea.append(messageToDisplay); // display message
                }
        );
    }

}
