package ch26_swing;

import java.awt.*;
import java.awt.event.MouseEvent;
import java.awt.event.MouseMotionAdapter;
import java.util.ArrayList;
import javax.swing.*;

// Fig. 26.35: Painter.java
// Testing PaintPanel.
public class Painter {
    public static void main(String[] args) {
        // create JFrame
        JFrame application = new JFrame("A simple paint program");

        PaintPanel paintPanel = new PaintPanel();
        application.add(paintPanel, BorderLayout.CENTER);

        // create a label and place it in SOUTH of BorderLayout
        application.add(new JLabel("Drag the mouse to draw"), BorderLayout.SOUTH);

        application.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        application.setSize(400, 200);
        application.setVisible(true);
    }
}


// Fig. 26.34: PaintPanel.java
// Adapter class used to implement event handlers.
class PaintPanel extends JPanel {
    // list Point references
    private final ArrayList<Point> points = new ArrayList<>();

    // set up GUI and register mouse event handler
    public PaintPanel() {
        // handle frame mouse motion event
        addMouseMotionListener(
                new MouseMotionAdapter() {
                    // store drag coordinates and repaint
                    @Override
                    public void mouseDragged(MouseEvent event) {
                        points.add(event.getPoint());
                        repaint(); // repaint JFrame
                    }
                }
        );
    }

    // draw ovals in a 4-by-4 bounding box at specified locations on window
    @Override
    public void paintComponent(Graphics g) {
        super.paintComponent(g); // clears drawing area

        // draw all
        for (Point point : points)
            g.fillOval(point.x, point.y, 4, 4);
    }
}
