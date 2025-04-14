package ch09.fig09_08;

// Fig. 9.8: BasePlusCommissionEmployee.java
// private superclass members cannot be accessed in a subclass.
public class BasePlusCommissionEmployee extends CommissionEmployee {
    private double baseSalary; // base salary per week

    public BasePlusCommissionEmployee(String firstName, String lastName,
                                      String socialSecurityNumber, double grossSales,
                                      double commissionRate, double baseSalary) {
        // explicit call to superclass CommissionEmployee constructor
        super(firstName, lastName, socialSecurityNumber, grossSales, commissionRate);

        // if baseSalary is invalid throw exception
        if (baseSalary < 0.0) {
            throw new IllegalArgumentException("Base salary must be >= 0.0");
        }

        this.baseSalary = baseSalary;
    }

    public void setBaseSalary(double baseSalary) {
        if (baseSalary < 0.0) {
            throw new IllegalArgumentException("Base salary must be >= 0.0");
        }
        this.baseSalary = baseSalary;
    }

    public double getBaseSalary() {
        return baseSalary;
    }
/*
    // calculate earnings
    @Override
    public double earnings() {
        // not allowed: commissionRate and grossSales private in superclass
        return baseSalary + (commissionRate * grossSales);
    }

    // return String representation of BasePlusCommissionEmployee
    @Override
    public String toString() {
        // not allowed: attempts to access private superclass members
        return String.format(
                "%s: %s %s%n%s: %s%n%s: %.2f%n%s: %.2f%n%s: %.2f",
                "base-salaried commission employee", firstName, lastName,
                "social security number", socialSecurityNumber,
                "gross sales", grossSales, "commission rate", commissionRate,
                "base salary", baseSalary);
    }
*/
} 
