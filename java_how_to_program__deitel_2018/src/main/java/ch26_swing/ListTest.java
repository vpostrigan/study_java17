package ch26_swing;

import javax.swing.*;
import java.awt.*;

// Fig. 26.24: ListTest.java
// Selecting colors from a JList.
public class ListTest {

    public static void main(String[] args) {
        ListFrame listFrame = new ListFrame(); // create ListFrame
        listFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        listFrame.setSize(350, 150);
        listFrame.setVisible(true);
    }

}


// Fig. 26.23: ListFrame.java
// JList that displays a list of colors.
class ListFrame extends JFrame {
    private final JList<String> colorJList; // list to display colors
    private static final String[] colorNames = {"Black", "Blue", "Cyan",
            "Dark Gray", "Gray", "Green", "Light Gray", "Magenta",
            "Orange", "Pink", "Red", "White", "Yellow"};
    private static final Color[] colors = {Color.BLACK, Color.BLUE,
            Color.CYAN, Color.DARK_GRAY, Color.GRAY, Color.GREEN,
            Color.LIGHT_GRAY, Color.MAGENTA, Color.ORANGE, Color.PINK,
            Color.RED, Color.WHITE, Color.YELLOW};

    // ListFrame constructor add JScrollPane containing JList to JFrame
    public ListFrame() {
        super("List Test");
        setLayout(new FlowLayout());

        colorJList = new JList<>(colorNames); // list of colorNames
        colorJList.setVisibleRowCount(5); // display five rows at once

        // do not allow multiple selections
        colorJList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

        // add a JScrollPane containing JList to frame
        add(new JScrollPane(colorJList));

        colorJList.addListSelectionListener(
                event -> getContentPane().setBackground(colors[colorJList.getSelectedIndex()]));
    }
}
