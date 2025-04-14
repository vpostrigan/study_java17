package ch19_swing;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

// Fig. 19.8: PopupTest.java
// Testing PopupFrame.
public class PopupTest {

    public static void main(String[] args) {
        PopupFrame popupFrame = new PopupFrame();
        popupFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        popupFrame.setSize(300, 200);
        popupFrame.setVisible(true);
    }


    // Fig. 19.7: PopupFrame.java
    // Demonstrating JPopupMenus.
    static class PopupFrame extends JFrame {
        private final JRadioButtonMenuItem[] items; // holds items for colors
        private final Color[] colorValues = {Color.BLUE, Color.YELLOW, Color.RED}; // colors to be used
        private final JPopupMenu popupMenu; // allows user to select color

        // no-argument constructor sets up GUI
        public PopupFrame() {
            super("Using JPopupMenus");

            PopupFrame.ItemHandler handler = new PopupFrame.ItemHandler(); // handler for menu items
            String[] colors = {"Blue", "Yellow", "Red"};

            ButtonGroup colorGroup = new ButtonGroup(); // manages color items
            popupMenu = new JPopupMenu(); // create pop-up menu
            items = new JRadioButtonMenuItem[colors.length];

            // construct menu item, add to pop-up menu, enable event handling
            for (int count = 0; count < items.length; count++) {
                items[count] = new JRadioButtonMenuItem(colors[count]);
                popupMenu.add(items[count]); // add item to pop-up menu
                colorGroup.add(items[count]); // add item to button group
                items[count].addActionListener(handler); // add handler
            }

            setBackground(Color.WHITE);

            // declare a MouseListener for the window to display pop-up menu
            addMouseListener(
                    new MouseAdapter() {
                        // handle mouse press event
                        @Override
                        public void mousePressed(MouseEvent event) {
                            checkForTriggerEvent(event);
                        }

                        // handle mouse release event
                        @Override
                        public void mouseReleased(MouseEvent event) {
                            checkForTriggerEvent(event);
                        }

                        // determine whether event should trigger pop-up menu
                        private void checkForTriggerEvent(MouseEvent event) {
                            if (event.isPopupTrigger())
                                popupMenu.show(event.getComponent(), event.getX(), event.getY());
                        }
                    }
            );
        }

        // private inner class to handle menu item events
        private class ItemHandler implements ActionListener {
            // process menu item selections
            @Override
            public void actionPerformed(ActionEvent event) {
                // determine which menu item was selected
                for (int i = 0; i < items.length; i++) {
                    if (event.getSource() == items[i]) {
                        getContentPane().setBackground(colorValues[i]);
                        return;
                    }
                }
            }
        }

    }


}
