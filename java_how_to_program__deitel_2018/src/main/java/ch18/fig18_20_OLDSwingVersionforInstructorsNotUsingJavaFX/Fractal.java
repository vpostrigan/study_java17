package ch18.fig18_20_OLDSwingVersionforInstructorsNotUsingJavaFX;

import java.awt.Color;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.JFrame;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JColorChooser;

// Fig. 18.19: Fractal.java
// Fractal user interface.
public class Fractal extends JFrame {
    private static final int WIDTH = 400;  // define width of GUI
    private static final int HEIGHT = 480; // define height of GUI
    private static final int MIN_LEVEL = 0;
    private static final int MAX_LEVEL = 15;

    // set up GUI
    public Fractal() {
        super("Fractal");

        // set up levelJLabel to add to controlJPanel
        final JLabel levelJLabel = new JLabel("Level: 0");

        final FractalJPanel drawSpace = new FractalJPanel(0);

        // set up control panel
        final JPanel controlJPanel = new JPanel();
        controlJPanel.setLayout(new FlowLayout());

        // set up color button and register listener
        final JButton changeColorJButton = new JButton("Color");
        controlJPanel.add(changeColorJButton);
        // anonymous inner class
        changeColorJButton.addActionListener(
                event -> {
                    // process changeColorJButton event
                    Color color = JColorChooser.showDialog(Fractal.this, "Choose a color", Color.BLUE);

                    // set default color, if no color is returned
                    if (color == null)
                        color = Color.BLUE;

                    drawSpace.setColor(color);
                }
        );

        // set up decrease level button to add to control panel and
        // register listener
        final JButton decreaseLevelJButton = new JButton("Decrease Level");
        controlJPanel.add(decreaseLevelJButton);
        // anonymous inner class
        decreaseLevelJButton.addActionListener(
                event -> {
                    // process decreaseLevelJButton event
                    int level = drawSpace.getLevel();
                    --level;

                    // modify level if possible
                    if ((level >= MIN_LEVEL) && (level <= MAX_LEVEL)) {
                        levelJLabel.setText("Level: " + level);
                        drawSpace.setLevel(level);
                        repaint();
                    }
                }
        );

        // set up increase level button to add to control panel
        // and register listener
        final JButton increaseLevelJButton = new JButton("Increase Level");
        controlJPanel.add(increaseLevelJButton);
        // anonymous inner class
        increaseLevelJButton.addActionListener(
                event -> {
                    // process increaseLevelJButton event
                    int level = drawSpace.getLevel();
                    ++level;

                    // modify level if possible
                    if ((level >= MIN_LEVEL) && (level <= MAX_LEVEL)) {
                        levelJLabel.setText("Level: " + level);
                        drawSpace.setLevel(level);
                        repaint();
                    }
                }
        );

        controlJPanel.add(levelJLabel);

        // create mainJPanel to contain controlJPanel and drawSpace
        final JPanel mainJPanel = new JPanel();
        mainJPanel.add(controlJPanel);
        mainJPanel.add(drawSpace);

        add(mainJPanel); // add JPanel to JFrame

        setSize(WIDTH, HEIGHT); // set size of JFrame
        setVisible(true); // display JFrame
    }

    public static void main(String[] args) {
        Fractal demo = new Fractal();
        demo.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

}
