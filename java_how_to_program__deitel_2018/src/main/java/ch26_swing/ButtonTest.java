package ch26_swing;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

// Fig. 26.16: ButtonTest.java
// Testing ButtonFrame.
public class ButtonTest {

    public static void main(String[] args) {
        ButtonFrame buttonFrame = new ButtonFrame();
        buttonFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        buttonFrame.setSize(275, 110);
        buttonFrame.setVisible(true);
    }

}

// Fig. 26.15: ButtonFrame.java
// Command buttons and action events.
class ButtonFrame extends JFrame {
    private final JButton plainJButton; // button with just text
    private final JButton fancyJButton; // button with icons

    // ButtonFrame adds JButtons to JFrame
    public ButtonFrame() {
        super("Testing Buttons");
        setLayout(new FlowLayout());

        plainJButton = new JButton("Plain Button"); // button with text
        add(plainJButton); // add plainJButton to JFrame

        Icon bug1 = new ImageIcon(getClass().getResource("bug1.gif"));
        Icon bug2 = new ImageIcon(getClass().getResource("bug2.gif"));
        fancyJButton = new JButton("Fancy Button", bug1); // set image
        fancyJButton.setRolloverIcon(bug2); // set rollover image
        add(fancyJButton); // add fancyJButton to JFrame

        // create new ButtonHandler for button event handling
        ButtonHandler handler = new ButtonHandler();
        fancyJButton.addActionListener(handler);
        plainJButton.addActionListener(handler);
    }

    // inner class for button event handling
    private class ButtonHandler implements ActionListener {
        // handle button event
        @Override
        public void actionPerformed(ActionEvent event) {
            JOptionPane.showMessageDialog(ButtonFrame.this,
                    String.format("You pressed: %s", event.getActionCommand()));
        }
    }

}
