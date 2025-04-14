package ch26_swing;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

// Fig. 26.26: MultipleSelectionTest.java
// Testing MultipleSelectionFrame.
public class MultipleSelectionTest {

    public static void main(String[] args) {
        MultipleSelectionFrame multipleSelectionFrame = new MultipleSelectionFrame();
        multipleSelectionFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        multipleSelectionFrame.setSize(350, 150);
        multipleSelectionFrame.setVisible(true);
    }

}


// Fig. 26.25: MultipleSelectionFrame.java
// JList that allows multiple selections.
class MultipleSelectionFrame extends JFrame {
    private final JList<String> colorJList; // list to hold color names
    private final JList<String> copyJList; // list to hold copied names
    private JButton copyJButton; // button to copy selected names
    private static final String[] colorNames = {"Black", "Blue", "Cyan",
            "Dark Gray", "Gray", "Green", "Light Gray", "Magenta", "Orange",
            "Pink", "Red", "White", "Yellow"};

    // MultipleSelectionFrame constructor
    public MultipleSelectionFrame() {
        super("Multiple Selection Lists");
        setLayout(new FlowLayout());

        colorJList = new JList<>(colorNames); // list of color names
        colorJList.setVisibleRowCount(5); // show five rows
        colorJList.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
        add(new JScrollPane(colorJList)); // add list with scrollpane

        copyJButton = new JButton("Copy >>>");
        copyJButton.addActionListener(
                new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent event) {
                        // place selected values in copyJList
                        copyJList.setListData(colorJList.getSelectedValuesList().toArray(new String[0]));
                    }
                }
        );

        add(copyJButton); // add copy button to JFrame

        copyJList = new JList<>(); // list to hold copied color names
        copyJList.setVisibleRowCount(5); // show 5 rows
        copyJList.setFixedCellWidth(100); // set width
        copyJList.setFixedCellHeight(15); // set height
        copyJList.setSelectionMode(ListSelectionModel.SINGLE_INTERVAL_SELECTION);
        add(new JScrollPane(copyJList)); // add list with scrollpane
    }

}
