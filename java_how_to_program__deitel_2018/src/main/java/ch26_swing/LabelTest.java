package ch26_swing;

import javax.swing.*;
import java.awt.*;

// Fig. 26.7: LabelTest.java
// Testing LabelFrame.
public class LabelTest {

    public static void main(String[] args) {
        LabelFrame labelFrame = new LabelFrame();
        labelFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        labelFrame.setSize(260, 180);
        labelFrame.setVisible(true);
    }

}


// Fig. 26.6: LabelFrame.java
// JLabels with text and icons.
class LabelFrame extends JFrame {
    private final JLabel label1; // JLabel with just text
    private final JLabel label2; // JLabel constructed with text and icon
    private final JLabel label3; // JLabel with added text and icon

    // LabelFrame constructor adds JLabels to JFrame
    public LabelFrame() {
        super("Testing JLabel");
        setLayout(new FlowLayout()); // set frame layout

        // JLabel constructor with a string argument
        label1 = new JLabel("Label with text");
        label1.setToolTipText("This is label1");
        add(label1); // add label1 to JFrame

        // JLabel constructor with string, Icon and alignment arguments
        Icon bug = new ImageIcon(getClass().getResource("bug1.png"));
        label2 = new JLabel("Label with text and icon", bug, SwingConstants.LEFT);
        label2.setToolTipText("This is label2");
        add(label2); // add label2 to JFrame

        label3 = new JLabel(); // JLabel constructor no arguments
        label3.setText("Label with icon and text at bottom");
        label3.setIcon(bug); // add icon to JLabel
        label3.setHorizontalTextPosition(SwingConstants.CENTER);
        label3.setVerticalTextPosition(SwingConstants.BOTTOM);
        label3.setToolTipText("This is label3");
        add(label3); // add label3 to JFrame
    }

}
