package edu.uoc.practica.bd.uocbugdb.exercise2;

public class Exercise2Row {
	private String cifCode;
	private String companyName;
	private float avgSalary;
	private int numInterviews;

	public Exercise2Row(String cifCode, String companyName, float avgSalary, int numInterviews) {
		super();
		this.cifCode = cifCode;
		this.companyName = companyName;
		this.avgSalary = avgSalary;
		this.numInterviews = numInterviews;
	}

	public String getCifCode() {
		return cifCode;
	}

	public void setCifCode(String cifCode) {
		this.cifCode = cifCode;
	}

	public String getCompanyName() {
		return companyName;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}

	public float getAvgSalary() {
		return avgSalary;
	}

	public void setAvgSalary(int avgSalary) {
		this.avgSalary = avgSalary;
	}

	public int getNumInterviews() {
		return numInterviews;
	}

	public void setNumInterviews(int numInterviews) {
		this.numInterviews = numInterviews;
	}

	
}
