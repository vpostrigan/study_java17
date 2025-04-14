package ch28_socket;

import javax.swing.*;
import javax.swing.event.HyperlinkEvent;
import java.awt.*;
import java.io.IOException;

// Fig. 28.2: ReadServerFileTest.java
// Create and start a ReadServerFile.
public class SimpleWebBrowser {

    public static void main(String[] args) {
        ReadServerFile application = new ReadServerFile();
        application.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

}

// Fig. 28.1: ReadServerFile.java
// Reading a file by opening a connection through a URL.
class ReadServerFile extends JFrame {
    private JTextField enterField; // JTextField to enter site name
    private JEditorPane contentsArea; // to display website

    // set up GUI
    public ReadServerFile() {
        super("Simple Web Browser");

        // create enterField and register its listener
        enterField = new JTextField("Enter file URL here");
        // get document specified by user
        enterField.addActionListener(
                event -> getThePage(event.getActionCommand())
        );

        add(enterField, BorderLayout.NORTH);

        contentsArea = new JEditorPane(); // create contentsArea
        contentsArea.setEditable(false);
        // if user clicked hyperlink, go to specified page
        contentsArea.addHyperlinkListener(
                event -> {
                    if (event.getEventType() == HyperlinkEvent.EventType.ACTIVATED)
                        getThePage(event.getURL().toString());
                }
        );

        add(new JScrollPane(contentsArea), BorderLayout.CENTER);
        setSize(400, 300); // set size of window
        setVisible(true); // show window
    }

    // load document
    private void getThePage(String location) {
        // load document and display location
        try {
            contentsArea.setPage(location); // set the page
            enterField.setText(location); // set the text
        } catch (IOException ioException) {
            JOptionPane.showMessageDialog(this, "Error retrieving specified URL", "Bad URL",
                    JOptionPane.ERROR_MESSAGE);
        }
    }

}
