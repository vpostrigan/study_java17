package ch26_swing;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

// Fig. 26.10: TextFieldTest.java
// Testing TextFieldFrame.
public class TextFieldTest {

    public static void main(String[] args) {
        TextFieldFrame textFieldFrame = new TextFieldFrame();
        textFieldFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        textFieldFrame.setSize(350, 100);
        textFieldFrame.setVisible(true);
    }

}

// Fig. 26.9: TextFieldFrame.java
// JTextFields and JPasswordFields.
class TextFieldFrame extends JFrame {
    private final JTextField textField1; // text field with set size
    private final JTextField textField2; // text field with text
    private final JTextField textField3; // text field with text and size
    private final JPasswordField passwordField; // password field with text

    // TextFieldFrame constructor adds JTextFields to JFrame
    public TextFieldFrame() {
        super("Testing JTextField and JPasswordField");
        setLayout(new FlowLayout());

        // construct textfield with 10 columns
        textField1 = new JTextField(10);
        add(textField1); // add textField1 to JFrame

        // construct textfield with default text
        textField2 = new JTextField("Enter text here");
        add(textField2); // add textField2 to JFrame

        // construct textfield with default text and 21 columns
        textField3 = new JTextField("Uneditable text field", 21);
        textField3.setEditable(false); // disable editing
        add(textField3); // add textField3 to JFrame

        // construct passwordfield with default text
        passwordField = new JPasswordField("Hidden text");
        add(passwordField); // add passwordField to JFrame

        // register event handlers
        TextFieldHandler handler = new TextFieldHandler();
        textField1.addActionListener(handler);
        textField2.addActionListener(handler);
        textField3.addActionListener(handler);
        passwordField.addActionListener(handler);
    }

    // private inner class for event handling
    private class TextFieldHandler implements ActionListener {
        // process textfield events
        @Override
        public void actionPerformed(ActionEvent event) {
            String string = "";

            if (event.getSource() == textField1) // user pressed Enter in JTextField textField1
                string = String.format("textField1: %s", event.getActionCommand());
            else if (event.getSource() == textField2) // user pressed Enter in JTextField textField2
                string = String.format("textField2: %s", event.getActionCommand());
            else if (event.getSource() == textField3) // user pressed Enter in JTextField textField3
                string = String.format("textField3: %s", event.getActionCommand());
            else if (event.getSource() == passwordField) // user pressed Enter in JTextField passwordField
                string = String.format("passwordField: %s", event.getActionCommand());

            // display JTextField content
            JOptionPane.showMessageDialog(null, string);
        }
    }

}
