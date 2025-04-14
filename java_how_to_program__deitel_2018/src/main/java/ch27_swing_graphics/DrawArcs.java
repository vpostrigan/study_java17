package ch27_swing_graphics;

import javax.swing.*;
import java.awt.*;

// Fig. 13.25: DrawArcs.java
// Arcs displayed with drawArc and fillArc.
public class DrawArcs {

    public static void main(String[] args) {
        // create frame for ArcsJPanel
        JFrame frame = new JFrame("Drawing Arcs");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        ArcsJPanel arcsJPanel = new ArcsJPanel();
        frame.add(arcsJPanel);
        frame.setSize(300, 210);
        frame.setVisible(true);
    }

}

// Fig. 13.24: ArcsJPanel.java
// Drawing arcs.
class ArcsJPanel extends JPanel {
    // draw rectangles and arcs
    @Override
    public void paintComponent(Graphics g) {
        super.paintComponent(g);

        // start at 0 and sweep 360 degrees
        g.setColor(Color.RED);
        g.drawRect(15, 35, 80, 80);
        g.setColor(Color.BLACK);
        g.drawArc(15, 35, 80, 80, 0, 360);

        // start at 0 and sweep 110 degrees
        g.setColor(Color.RED);
        g.drawRect(100, 35, 80, 80);
        g.setColor(Color.BLACK);
        g.drawArc(100, 35, 80, 80, 0, 110);

        // start at 0 and sweep -270 degrees
        g.setColor(Color.RED);
        g.drawRect(185, 35, 80, 80);
        g.setColor(Color.BLACK);
        g.drawArc(185, 35, 80, 80, 0, -270);

        // start at 0 and sweep 360 degrees
        g.fillArc(15, 120, 80, 40, 0, 360);

        // start at 270 and sweep -90 degrees
        g.fillArc(100, 120, 80, 40, 270, -90);

        // start at 0 and sweep -270 degrees
        g.fillArc(185, 120, 80, 40, 0, -270);
    }
}