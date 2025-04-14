package ch26_swing;

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;

// Fig. 26.29: MouseTrackerFrame.java
// Testing MouseTrackerFrame.
public class MouseTracker {

    public static void main(String[] args) {
        MouseTrackerFrame mouseTrackerFrame = new MouseTrackerFrame();
        mouseTrackerFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        mouseTrackerFrame.setSize(300, 100);
        mouseTrackerFrame.setVisible(true);
    }

}

// Fig. 26.28: MouseTrackerFrame.java
// Mouse event handling.
class MouseTrackerFrame extends JFrame {
    private final JPanel mousePanel; // panel in which mouse events occur
    private final JLabel statusBar; // displays event information

    // MouseTrackerFrame constructor sets up GUI and
    // registers mouse event handlers
    public MouseTrackerFrame() {
        super("Demonstrating Mouse Events");

        mousePanel = new JPanel();
        mousePanel.setBackground(Color.WHITE);
        add(mousePanel, BorderLayout.CENTER); // add panel to JFrame

        statusBar = new JLabel("Mouse outside JPanel");
        add(statusBar, BorderLayout.SOUTH); // add label to JFrame

        // create and register listener for mouse and mouse motion events
        MouseHandler handler = new MouseHandler();
        mousePanel.addMouseListener(handler);
        mousePanel.addMouseMotionListener(handler);
    }

    private class MouseHandler implements MouseListener, MouseMotionListener {
        // MouseListener event handlers
        // handle event when mouse released immediately after press
        @Override
        public void mouseClicked(MouseEvent event) {
            statusBar.setText(String.format("Clicked at [%d, %d]", event.getX(), event.getY()));
        }

        // handle event when mouse pressed
        @Override
        public void mousePressed(MouseEvent event) {
            statusBar.setText(String.format("Pressed at [%d, %d]", event.getX(), event.getY()));
        }

        // handle event when mouse released
        @Override
        public void mouseReleased(MouseEvent event) {
            statusBar.setText(String.format("Released at [%d, %d]", event.getX(), event.getY()));
        }

        // handle event when mouse enters area
        @Override
        public void mouseEntered(MouseEvent event) {
            statusBar.setText(String.format("Mouse entered at [%d, %d]", event.getX(), event.getY()));
            mousePanel.setBackground(Color.GREEN);
        }

        // handle event when mouse exits area
        @Override
        public void mouseExited(MouseEvent event) {
            statusBar.setText("Mouse outside JPanel");
            mousePanel.setBackground(Color.WHITE);
        }

        // MouseMotionListener event handlers
        // handle event when user drags mouse with button pressed
        @Override
        public void mouseDragged(MouseEvent event) {
            statusBar.setText(String.format("Dragged at [%d, %d]", event.getX(), event.getY()));
        }

        // handle event when user moves mouse
        @Override
        public void mouseMoved(MouseEvent event) {
            statusBar.setText(String.format("Moved at [%d, %d]", event.getX(), event.getY()));
        }
    }

}
