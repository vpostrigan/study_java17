package ch22.ThreeDimensionalShapes;

import javafx.fxml.FXML;
import javafx.scene.paint.Color;
import javafx.scene.paint.PhongMaterial;
import javafx.scene.image.Image;
import javafx.scene.shape.Box;
import javafx.scene.shape.Cylinder;
import javafx.scene.shape.Sphere;

// Fig. 22.15: ThreeDimensionalShapesController.java
// Setting the material displayed on 3D shapes.

public class ThreeDimensionalShapesController {
    // instance variables that refer to 3D shapes
    @FXML
    private Box box;
    @FXML
    private Cylinder cylinder;
    @FXML
    private Sphere sphere;

    // set the material for each 3D shape
    public void initialize() {
        // define material for the Box object
        PhongMaterial boxMaterial = new PhongMaterial();
        boxMaterial.setDiffuseColor(Color.CYAN);
        box.setMaterial(boxMaterial);

        // define material for the Cylinder object
        PhongMaterial cylinderMaterial = new PhongMaterial();
        cylinderMaterial.setDiffuseMap(new Image("yellowflowers.png"));
        cylinder.setMaterial(cylinderMaterial);

        // define material for the Sphere object
        PhongMaterial sphereMaterial = new PhongMaterial();
        sphereMaterial.setDiffuseColor(Color.RED);
        sphereMaterial.setSpecularColor(Color.WHITE);
        sphereMaterial.setSpecularPower(32);
        sphere.setMaterial(sphereMaterial);
    }

}
