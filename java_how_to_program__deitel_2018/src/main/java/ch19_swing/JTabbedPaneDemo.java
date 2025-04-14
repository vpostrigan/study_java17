package ch19_swing;

import javax.swing.*;
import java.awt.*;

// Fig. 19.14: JTabbedPaneDemo.java
// Demonstrating JTabbedPane.
public class JTabbedPaneDemo {

    public static void main(String[] args) {
        JTabbedPaneFrame tabbedPaneFrame = new JTabbedPaneFrame();
        tabbedPaneFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        tabbedPaneFrame.setSize(250, 200);
        tabbedPaneFrame.setVisible(true);
    }

    // Fig. 19.13: JTabbedPaneFrame.java
    // Demonstrating JTabbedPane.
    static class JTabbedPaneFrame extends JFrame {

        // set up GUI
        public JTabbedPaneFrame() {
            super("JTabbedPane Demo ");

            JTabbedPane tabbedPane = new JTabbedPane(); // create JTabbedPane

            // set up pane11 and add it to JTabbedPane
            JLabel label1 = new JLabel("panel one", SwingConstants.CENTER);
            JPanel panel1 = new JPanel();
            panel1.add(label1);
            tabbedPane.addTab("Tab One", null, panel1, "First Panel");

            // set up panel2 and add it to JTabbedPane
            JLabel label2 = new JLabel("panel two", SwingConstants.CENTER);
            JPanel panel2 = new JPanel();
            panel2.setBackground(Color.YELLOW);
            panel2.add(label2);
            tabbedPane.addTab("Tab Two", null, panel2, "Second Panel");

            // set up panel3 and add it to JTabbedPane
            JLabel label3 = new JLabel("panel three");
            JPanel panel3 = new JPanel();
            panel3.setLayout(new BorderLayout());
            panel3.add(new JButton("North"), BorderLayout.NORTH);
            panel3.add(new JButton("West"), BorderLayout.WEST);
            panel3.add(new JButton("East"), BorderLayout.EAST);
            panel3.add(new JButton("South"), BorderLayout.SOUTH);
            panel3.add(label3, BorderLayout.CENTER);
            tabbedPane.addTab("Tab Three", null, panel3, "Third Panel");

            add(tabbedPane); // add JTabbedPane to frame
        }

    }

}
