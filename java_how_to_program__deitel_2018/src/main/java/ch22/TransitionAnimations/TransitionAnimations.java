package ch22.TransitionAnimations;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

// TransitionAnimations.java

public class TransitionAnimations extends Application {

    @Override
    public void start(Stage stage) throws Exception {
        Parent root = FXMLLoader.load(getClass().getResource("TransitionAnimations.fxml"));

        Scene scene = new Scene(root);
        stage.setTitle("TransitionAnimations");
        stage.setScene(scene);
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }

}
