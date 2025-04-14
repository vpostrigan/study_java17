package ch23.Fig23_23;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;

// Fig. 23.26: FibonacciNumbersController.java
// Using a Task to perform a long calculation
// outside the JavaFX application thread.
public class FibonacciNumbersController {
    @FXML
    private TextField numberTextField;
    @FXML
    private Button goButton;
    @FXML
    private Label messageLabel;
    @FXML
    private Label fibonacciLabel;
    @FXML
    private Label nthLabel;
    @FXML
    private Label nthFibonacciLabel;

    private long n1 = 0; // initialize with Fibonacci of 0
    private long n2 = 1; // initialize with Fibonacci of 1
    private int number = 1; // current Fibonacci number to display

    // starts FibonacciTask to calculate in background
    @FXML
    void goButtonPressed(ActionEvent event) {
        // get Fibonacci number to calculate
        try {
            int input = Integer.parseInt(numberTextField.getText());

            // create, configure and launch FibonacciTask
            FibonacciTask task = new FibonacciTask(input);

            // display task's messages in messageLabel
            messageLabel.textProperty().bind(task.messageProperty());

            // clear fibonacciLabel when task starts
            task.setOnRunning((succeededEvent) -> {
                goButton.setDisable(true);
                fibonacciLabel.setText("");
            });

            // set fibonacciLabel when task completes successfully
            task.setOnSucceeded((succeededEvent) -> {
                fibonacciLabel.setText(task.getValue().toString());
                goButton.setDisable(false);
            });

            // create ExecutorService to manage threads
            ExecutorService executorService = Executors.newFixedThreadPool(1); // pool of one thread
            executorService.execute(task); // start the task
            executorService.shutdown();
        } catch (NumberFormatException e) {
            numberTextField.setText("Enter an integer");
            numberTextField.selectAll();
            numberTextField.requestFocus();
        }
    }

    // calculates next Fibonacci value
    @FXML
    void nextNumberButtonPressed(ActionEvent event) {
        // display the next Fibonacci number
        nthLabel.setText("Fibonacci of " + number + ": ");
        nthFibonacciLabel.setText(String.valueOf(n2));
        long temp = n1 + n2;
        n1 = n2;
        n2 = temp;
        ++number;
    }

}
