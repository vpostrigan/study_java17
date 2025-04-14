package ch27_swing_graphics;

import java.awt.*;
import javax.swing.*;

// Fig. 13.19: LinesRectsOvals.java
// Testing LinesRectsOvalsJPanel.
public class LinesRectsOvals {

    public static void main(String[] args) {
        // create frame for LinesRectsOvalsJPanel
        JFrame frame = new JFrame("Drawing lines, rectangles and ovals");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        LinesRectsOvalsJPanel linesRectsOvalsJPanel = new LinesRectsOvalsJPanel();
        linesRectsOvalsJPanel.setBackground(Color.WHITE);
        frame.add(linesRectsOvalsJPanel);
        frame.setSize(400, 210);
        frame.setVisible(true);
    }

}


// Fig. 13.18: LinesRectsOvalsJPanel.java
// Drawing lines, rectangles and ovals.
class LinesRectsOvalsJPanel extends JPanel {
    // display various lines, rectangles and ovals
    @Override
    public void paintComponent(Graphics g) {
        super.paintComponent(g);
        this.setBackground(Color.WHITE);

        g.setColor(Color.RED);
        g.drawLine(5, 30, 380, 30);

        g.setColor(Color.BLUE);
        g.drawRect(5, 40, 90, 55);
        g.fillRect(100, 40, 90, 55);

        g.setColor(Color.BLACK);
        g.fillRoundRect(195, 40, 90, 55, 50, 50);
        g.drawRoundRect(290, 40, 90, 55, 20, 20);

        g.setColor(Color.GREEN);
        g.draw3DRect(5, 100, 90, 55, true);
        g.fill3DRect(100, 100, 90, 55, false);

        g.setColor(Color.MAGENTA);
        g.drawOval(195, 100, 90, 55);
        g.fillOval(290, 100, 90, 55);
    }
}