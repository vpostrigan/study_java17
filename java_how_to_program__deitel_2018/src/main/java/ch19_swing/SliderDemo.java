package ch19_swing;

import javax.swing.*;
import java.awt.*;

// Fig. 19.4: SliderDemo.java
// Testing SliderFrame.
public class SliderDemo {

    public static void main(String[] args) {
        SliderFrame sliderFrame = new SliderFrame();
        sliderFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        sliderFrame.setSize(220, 270);
        sliderFrame.setVisible(true);
    }

    // Fig. 19.3: SliderFrame.java
    // Using JSliders to size an oval.
    static class SliderFrame extends JFrame {
        private final JSlider diameterJSlider; // slider to select diameter
        private final OvalPanel myPanel; // panel to draw circle

        // no-argument constructor
        public SliderFrame() {
            super("Slider Demo");

            myPanel = new OvalPanel(); // create panel to draw circle
            myPanel.setBackground(Color.YELLOW);

            // set up JSlider to control diameter value
            diameterJSlider = new JSlider(SwingConstants.HORIZONTAL, 0, 200, 10);
            diameterJSlider.setMajorTickSpacing(10); // create tick every 10
            diameterJSlider.setPaintTicks(true); // paint ticks on slider

            // register JSlider event listener
            // anonymous inner class
            diameterJSlider.addChangeListener(
                    // handle change in slider value
                    e -> myPanel.setDiameter(diameterJSlider.getValue())
            );

            add(diameterJSlider, BorderLayout.SOUTH);
            add(myPanel, BorderLayout.CENTER);
        }
    }

    // Fig. 19.2: OvalPanel.java
    // A customized JPanel class.
    static class OvalPanel extends JPanel {
        private int diameter = 10; // default diameter

        // draw an oval of the specified diameter
        @Override
        public void paintComponent(Graphics g) {
            super.paintComponent(g);
            g.fillOval(10, 10, diameter, diameter);
        }

        // validate and set diameter, then repaint
        public void setDiameter(int newDiameter) {
            // if diameter invalid, default to 10
            diameter = (newDiameter >= 0 ? newDiameter : 10);
            repaint(); // repaint panel
        }

        // used by layout manager to determine preferred size
        public Dimension getPreferredSize() {
            return new Dimension(200, 200);
        }

        // used by layout manager to determine minimum size
        public Dimension getMinimumSize() {
            return getPreferredSize();
        }
    }

}
