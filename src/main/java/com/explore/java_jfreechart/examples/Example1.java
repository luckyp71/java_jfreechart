package com.explore.java_jfreechart.examples;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.general.PieDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datavec.spark.transform.misc.StringToWritablesFunction;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.writable.Writable;
import org.datavec.api.transform.schema.Schema;
import org.nd4j.linalg.io.ClassPathResource;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.swing.JPanel;

/*
 * Create pie chart based on csv file
 */

public class Example1 extends ApplicationFrame {
	
	public Example1(String title) {
		super(title);
	}

	public static void main (String[] args)  {
		
		String salesPath = "";
		
		try {
			salesPath = new ClassPathResource("sales.csv").getFile().getPath();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//Define dataset
		Schema salesSchema = new Schema.Builder()
				.addColumnString("Product")
				.addColumnInteger("Qty")
				.build();
		
		//Spark Config
		SparkConf conf = new SparkConf();
		conf.setMaster("local[*]");
		conf.setAppName("Jfreechart example 1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Load file
		RecordReader rr = new CSVRecordReader();
		JavaRDD<List<Writable>> salesInfo = sc.textFile(salesPath).map(new StringToWritablesFunction(rr));
		
		//Collect Data
		List<List<Writable>> salesInfoList = salesInfo.collect();
		sc.stop();
	
		//Display Data
		System.out.println(salesSchema.getName(0)+"\t\t"+salesSchema.getName(1));
		for(List<Writable> line: salesInfoList) {
			System.out.println(line.get(0)+"\t\t"+line.get(1));
		}
		
		//Display Chart
		String title = "Sales Data";
		Example1 example1 = new Example1(title);
		example1.setContentPane(createChartPanel(title, pieDataset(salesInfoList)));
		RefineryUtilities.centerFrameOnScreen(example1);
		example1.setSize(800, 600);
		example1.setAlwaysOnTop(true);
		example1.setLocation(0, 0);
		example1.setVisible(true);

		//Save sales chart 
		int width = 800;
		int height = 600;
		try {
			ChartUtilities.saveChartAsPNG(salesChartToSave("sales_chart.png"), createChart(title, pieDataset(salesInfoList)), width, height);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//Setup pie dataset
	private static PieDataset pieDataset(List<List<Writable>> listData) {
		DefaultPieDataset data = new DefaultPieDataset();
		
		for(List<Writable> datum: listData) {
			data.setValue(String.valueOf(datum.get(0)), Long.parseLong(String.valueOf(datum.get(1))));
		}
		return data;
	}
	
	//Create chart
	private static JFreeChart createChart(String title,PieDataset data) {
		JFreeChart chart = ChartFactory.createPieChart(title, data,true, true, false);
		return chart;
	}
	
	//Create chart panel
	private static JPanel createChartPanel(String title, PieDataset dataset) {
		JFreeChart pieChart = createChart(title,dataset);
		return new ChartPanel(pieChart);
	}	
	
	//Save file
	private static File salesChartToSave(String name) {
		File fileName = new File("src/main/resources/"+name);
		return fileName;
	}
}
