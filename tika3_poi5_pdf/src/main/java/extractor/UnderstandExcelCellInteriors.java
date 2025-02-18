package extractor;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;

/**
 * https://stackoverflow.com/questions/75682944/use-setfillbackgroundcolor-and-setfillforegroundcolor-together
 */
public class UnderstandExcelCellInteriors {

    public static void main(String[] args) throws Exception {
        Workbook workbook = new XSSFWorkbook();
        FileOutputStream out = new FileOutputStream("UnderstandExcelCellInteriors.xlsx");

        CellStyle cellStyle = workbook.createCellStyle();
        cellStyle.setAlignment(HorizontalAlignment.CENTER);
        cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);

        Font whiteFont = workbook.createFont();
        whiteFont.setColor(IndexedColors.WHITE.getIndex());

        CellStyle cellStyle1 = workbook.createCellStyle();
        cellStyle1.cloneStyleFrom(cellStyle);
        cellStyle1.setFillForegroundColor(IndexedColors.BRIGHT_GREEN.getIndex()); //FillForegroundColor is the color of the pattern
        cellStyle1.setFillPattern(FillPatternType.SOLID_FOREGROUND); //solid pattern means cell fill is FillForegroundColor only

        CellStyle cellStyle2 = workbook.createCellStyle();
        cellStyle2.cloneStyleFrom(cellStyle);
        cellStyle2.setFillBackgroundColor(IndexedColors.YELLOW.getIndex()); //FillBackgroundColor is the color behind the pattern
        cellStyle2.setFillForegroundColor(IndexedColors.BLUE.getIndex()); //FillForegroundColor is the color of the pattern
        cellStyle2.setFillPattern(FillPatternType.BIG_SPOTS); //big spots pattern in blue in front of yellow background

        CellStyle cellStyle3 = workbook.createCellStyle();
        cellStyle3.cloneStyleFrom(cellStyle);
        cellStyle3.setFillForegroundColor(IndexedColors.GREEN.getIndex()); //FillForegroundColor is the color of the pattern
        cellStyle3.setFillPattern(FillPatternType.SOLID_FOREGROUND); //solid pattern means cell fill is FillForegroundColor only
        cellStyle3.setFont(whiteFont); //white text color via font

        Sheet sheet = workbook.createSheet();
        Row row = sheet.createRow(0);

        Cell cell = row.createCell(0);
        cell.setCellValue("cell value");
        cell.setCellStyle(cellStyle1);

        cell = row.createCell(1);
        cell.setCellValue("cell value");
        cell.setCellStyle(cellStyle2);

        cell = row.createCell(2);
        cell.setCellValue("cell value");
        cell.setCellStyle(cellStyle3);

        row.setHeightInPoints(50);
        sheet.setColumnWidth(0, 50 * 256);
        sheet.setColumnWidth(1, 50 * 256);
        sheet.setColumnWidth(2, 50 * 256);

        workbook.write(out);
        out.close();
        workbook.close();
    }

}
