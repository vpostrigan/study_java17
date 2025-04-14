package ch26_swing;

import javax.swing.*;
import java.awt.*;

// Fig. 26.46: PanelDemo.java
// Testing PanelFrame.
public class PanelDemo extends JFrame {

    public static void main(String[] args) {
        PanelFrame panelFrame = new PanelFrame();
        panelFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        panelFrame.setSize(450, 200);
        panelFrame.setVisible(true);
    }

}

// Fig. 26.45: PanelFrame.java
// Using a JPanel to help lay out components.
class PanelFrame extends JFrame {
    private final JPanel buttonJPanel; // panel to hold buttons
    private final JButton[] buttons;

    // no-argument constructor
    public PanelFrame() {
        super("Panel Demo");
        buttons = new JButton[5];
        buttonJPanel = new JPanel();
        buttonJPanel.setLayout(new GridLayout(1, buttons.length));

        // create and add buttons
        for (int count = 0; count < buttons.length; count++) {
            buttons[count] = new JButton("Button " + (count + 1));
            buttonJPanel.add(buttons[count]); // add button to panel
        }

        add(buttonJPanel, BorderLayout.SOUTH); // add panel to JFrame
    }
}
