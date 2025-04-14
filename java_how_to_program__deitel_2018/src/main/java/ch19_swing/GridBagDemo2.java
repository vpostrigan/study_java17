package ch19_swing;

import javax.swing.*;
import java.awt.*;

// Fig. 19.24: GridBagDemo2.java
// Demonstrating GridBagLayout constants.
public class GridBagDemo2 {

    public static void main(String[] args) {
        GridBagFrame2 gridBagFrame = new GridBagFrame2();
        gridBagFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        gridBagFrame.setSize(300, 200);
        gridBagFrame.setVisible(true);
    }

    // Fig. 19.23: GridBagFrame2.java
    // Demonstrating GridBagLayout constants.
    static class GridBagFrame2 extends JFrame {
        private final GridBagLayout layout; // layout of this frame
        private final GridBagConstraints constraints; // layout's constraints

        // set up GUI
        public GridBagFrame2() {
            super("GridBagLayout");
            layout = new GridBagLayout();
            setLayout(layout);
            constraints = new GridBagConstraints(); // instantiate constraints

            // create GUI components
            String[] metals = {"Copper", "Aluminum", "Silver"};
            JComboBox<String> comboBox = new JComboBox<>(metals);

            JTextField textField = new JTextField("TextField");

            String[] fonts = {"Serif", "Monospaced"};
            JList list = new JList(fonts);

            String[] names = {"zero", "one", "two", "three", "four"};
            JButton[] buttons = new JButton[names.length];

            for (int count = 0; count < buttons.length; count++)
                buttons[count] = new JButton(names[count]);

            // define GUI component constraints for textField
            constraints.weightx = 1;
            constraints.weighty = 1;
            constraints.fill = GridBagConstraints.BOTH;
            constraints.gridwidth = GridBagConstraints.REMAINDER;
            addComponent(textField);

            // buttons[0] -- weightx and weighty are 1: fill is BOTH
            constraints.gridwidth = 1;
            addComponent(buttons[0]);

            // buttons[1] -- weightx and weighty are 1: fill is BOTH
            constraints.gridwidth = GridBagConstraints.RELATIVE;
            addComponent(buttons[1]);

            // buttons[2] -- weightx and weighty are 1: fill is BOTH
            constraints.gridwidth = GridBagConstraints.REMAINDER;
            addComponent(buttons[2]);

            // comboBox -- weightx is 1: fill is BOTH
            constraints.weighty = 0;
            constraints.gridwidth = GridBagConstraints.REMAINDER;
            addComponent(comboBox);

            // buttons[3] -- weightx is 1: fill is BOTH
            constraints.weighty = 1;
            constraints.gridwidth = GridBagConstraints.REMAINDER;
            addComponent(buttons[3]);

            // buttons[4] -- weightx and weighty are 1: fill is BOTH
            constraints.gridwidth = GridBagConstraints.RELATIVE;
            addComponent(buttons[4]);

            // list -- weightx and weighty are 1: fill is BOTH
            constraints.gridwidth = GridBagConstraints.REMAINDER;
            addComponent(list);
        }

        // add a component to the container
        private void addComponent(Component component) {
            layout.setConstraints(component, constraints);
            add(component); // add component
        }
    }

}
