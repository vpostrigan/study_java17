package ch27_swing_graphics;

import javax.swing.*;
import java.awt.*;

// Fig. 13.8: ShowColors2.java
// Choosing colors with JColorChooser.
public class ShowColors2 {

    public static void main(String[] args) {
        ShowColors2JFrame application = new ShowColors2JFrame();
        application.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

}


// Fig. 13.7: ShowColors2JFrame.java
// Choosing colors with JColorChooser.
class ShowColors2JFrame extends JFrame {
    private final JButton changeColorJButton;
    private Color color = Color.LIGHT_GRAY;
    private final JPanel colorJPanel;

    // set up GUI
    public ShowColors2JFrame() {
        super("Using JColorChooser");

        // create JPanel for display color
        colorJPanel = new JPanel();
        colorJPanel.setBackground(color);

        // set up changeColorJButton and register its event handler
        changeColorJButton = new JButton("Change Color");
        changeColorJButton.addActionListener(
                event -> {
                    // display JColorChooser when user clicks button
                    color = JColorChooser.showDialog(ShowColors2JFrame.this, "Choose a color", color);

                    // set default color, if no color is returned
                    if (color == null)
                        color = Color.LIGHT_GRAY;

                    // change content pane's background color
                    colorJPanel.setBackground(color);
                }
        );

        add(colorJPanel, BorderLayout.CENTER);
        add(changeColorJButton, BorderLayout.SOUTH);

        setSize(400, 130);
        setVisible(true);
    }

}
