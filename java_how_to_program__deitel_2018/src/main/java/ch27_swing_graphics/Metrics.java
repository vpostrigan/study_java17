package ch27_swing_graphics;

import javax.swing.*;
import java.awt.*;

// Fig. 13.16: Metrics.java
// Displaying font metrics.
public class Metrics {

    public static void main(String[] args) {
        // create frame for MetricsJPanel
        JFrame frame = new JFrame("Demonstrating FontMetrics");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        MetricsJPanel metricsJPanel = new MetricsJPanel();
        frame.add(metricsJPanel);
        frame.setSize(510, 240);
        frame.setVisible(true);
    }

}


// Fig. 13.15: MetricsJPanel.java
// FontMetrics and Graphics methods useful for obtaining font metrics.
class MetricsJPanel extends JPanel {
    // display font metrics
    @Override
    public void paintComponent(Graphics g) {
        super.paintComponent(g);

        g.setFont(new Font("SansSerif", Font.BOLD, 12));
        FontMetrics metrics = g.getFontMetrics();
        g.drawString("Current font: " + g.getFont(), 10, 30);
        g.drawString("Ascent: " + metrics.getAscent(), 10, 45);
        g.drawString("Descent: " + metrics.getDescent(), 10, 60);
        g.drawString("Height: " + metrics.getHeight(), 10, 75);
        g.drawString("Leading: " + metrics.getLeading(), 10, 90);

        Font font = new Font("Serif", Font.ITALIC, 14);
        metrics = g.getFontMetrics(font);
        g.setFont(font);
        g.drawString("Current font: " + font, 10, 120);
        g.drawString("Ascent: " + metrics.getAscent(), 10, 135);
        g.drawString("Descent: " + metrics.getDescent(), 10, 150);
        g.drawString("Height: " + metrics.getHeight(), 10, 165);
        g.drawString("Leading: " + metrics.getLeading(), 10, 180);
    }
}
