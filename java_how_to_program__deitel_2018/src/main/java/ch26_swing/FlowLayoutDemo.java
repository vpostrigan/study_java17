package ch26_swing;

import javax.swing.*;
import java.awt.*;

// Fig. 26.40: FlowLayoutDemo.java
// Testing FlowLayoutFrame.
public class FlowLayoutDemo {

    public static void main(String[] args) {
        FlowLayoutFrame flowLayoutFrame = new FlowLayoutFrame();
        flowLayoutFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        flowLayoutFrame.setSize(300, 75);
        flowLayoutFrame.setVisible(true);
    }

}


// Fig. 26.39: FlowLayoutFrame.java
// FlowLayout allows components to flow over multiple lines.
class FlowLayoutFrame extends JFrame {
    private final JButton leftJButton; // button to set alignment left
    private final JButton centerJButton; // button to set alignment center
    private final JButton rightJButton; // button to set alignment right
    private final FlowLayout layout; // layout object
    private final Container container; // container to set layout

    // set up GUI and register button listeners
    public FlowLayoutFrame() {
        super("FlowLayout Demo");

        layout = new FlowLayout();
        container = getContentPane(); // get container to layout
        setLayout(layout);

        // set up leftJButton and register listener
        leftJButton = new JButton("Left");
        add(leftJButton); // add Left button to frame
        leftJButton.addActionListener(
                event -> {
                    layout.setAlignment(FlowLayout.LEFT);

                    // realign attached components
                    layout.layoutContainer(container);
                }
        );

        // set up centerJButton and register listener
        centerJButton = new JButton("Center");
        add(centerJButton); // add Center button to frame
        centerJButton.addActionListener(
                event -> {
                    layout.setAlignment(FlowLayout.CENTER);

                    // realign attached components
                    layout.layoutContainer(container);
                }
        );

        // set up rightJButton and register listener
        rightJButton = new JButton("Right"); // create Right button
        add(rightJButton); // add Right button to frame
        rightJButton.addActionListener(
                event -> {
                    layout.setAlignment(FlowLayout.RIGHT);

                    // realign attached components
                    layout.layoutContainer(container);
                }
        );
    }
}
