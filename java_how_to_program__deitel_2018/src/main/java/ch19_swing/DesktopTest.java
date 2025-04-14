package ch19_swing;

import javax.swing.*;
import java.awt.*;
import java.security.SecureRandom;

// Fig. 19.12: DesktopTest.java
// Demonstrating JDesktopPane.
public class DesktopTest {

    public static void main(String[] args) {
        DesktopFrame desktopFrame = new DesktopFrame();
        desktopFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        desktopFrame.setSize(600, 480);
        desktopFrame.setVisible(true);
    }

}

// Fig. 19.11: DesktopFrame.java
// Demonstrating JDesktopPane.
class DesktopFrame extends JFrame {
    private final JDesktopPane theDesktop;

    // set up GUI
    public DesktopFrame() {
        super("Using a JDesktopPane");

        JMenuBar bar = new JMenuBar();
        JMenu addMenu = new JMenu("Add");
        JMenuItem newFrame = new JMenuItem("Internal Frame");

        addMenu.add(newFrame); // add new frame item to Add menu
        bar.add(addMenu); // add Add menu to menu bar
        setJMenuBar(bar); // set menu bar for this application

        theDesktop = new JDesktopPane();
        add(theDesktop); // add desktop pane to frame

        // set up listener for newFrame menu item
        newFrame.addActionListener(
                event -> { // display new internal window
                    // create internal frame
                    JInternalFrame frame = new JInternalFrame(
                            "Internal Frame", true, true, true, true);

                    MyJPanel panel = new MyJPanel();
                    frame.add(panel, BorderLayout.CENTER);
                    frame.pack(); // set internal frame to size of contents

                    theDesktop.add(frame); // attach internal frame
                    frame.setVisible(true); // show internal frame
                }
        );
    }
}

// class to display an ImageIcon on a panel
class MyJPanel extends JPanel {
    private static final SecureRandom generator = new SecureRandom();
    private final ImageIcon picture; // image to be displayed
    private static final String[] images = {"yellowflowers.png",
            "purpleflowers.png", "redflowers.png", "redflowers2.png",
            "lavenderflowers.png"};

    // load image
    public MyJPanel() {
        int randomNumber = generator.nextInt(images.length);
        picture = new ImageIcon(images[randomNumber]); // set icon
    }

    // display imageIcon on panel
    @Override
    public void paintComponent(Graphics g) {
        super.paintComponent(g);
        picture.paintIcon(this, g, 0, 0); // display icon
    }

    // return image dimensions
    public Dimension getPreferredSize() {
        return new Dimension(picture.getIconWidth(), picture.getIconHeight());
    }
}
