package ch26_swing;

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

// Fig. 26.32: MouseDetails.java
// Testing MouseDetailsFrame.
public class MouseDetails {

    public static void main(String[] args) {
        MouseDetailsFrame mouseDetailsFrame = new MouseDetailsFrame();
        mouseDetailsFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        mouseDetailsFrame.setSize(400, 150);
        mouseDetailsFrame.setVisible(true);
    }

}


// Fig. 26.31: MouseDetailsFrame.java
// Demonstrating mouse clicks and distinguishing between mouse buttons.
class MouseDetailsFrame extends JFrame {
    private String details; // String displayed in the statusBar
    private final JLabel statusBar; // JLabel at bottom of window

    // constructor sets title bar String and register mouse listener
    public MouseDetailsFrame() {
        super("Mouse Clicks and Buttons");

        statusBar = new JLabel("Click the mouse");
        add(statusBar, BorderLayout.SOUTH);
        addMouseListener(new MouseClickHandler()); // add handler
    }

    // inner class to handle mouse events
    private class MouseClickHandler extends MouseAdapter {
        // handle mouse-click event and determine which button was pressed
        @Override
        public void mouseClicked(MouseEvent event) {
            int xPos = event.getX(); // get x-position of mouse
            int yPos = event.getY(); // get y-position of mouse

            details = String.format("Clicked %d time(s)", event.getClickCount());

            if (event.isMetaDown()) // right mouse button
                details += " with right mouse button";
            else if (event.isAltDown()) // middle mouse button
                details += " with center mouse button";
            else // left mouse button
                details += " with left mouse button";

            statusBar.setText(details);
        }
    }
}
