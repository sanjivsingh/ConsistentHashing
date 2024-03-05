package com.sanjivsingh.consistenthashing;

/**
 * The Class Driver.
 */
public class Driver {
  
  /**
   * The main method.
   *
   * @param args the arguments
   * @throws Exception the exception
   */
  public static void main(String[] args) throws Exception {
    
    // nodeInstances holding instances of actual storage node objects
    Server s1 = new Server("S1", "10.131.20.20");
    Server s2 = new Server("S2", "10.131.20.10");
    Server s3 = new Server("S3", "10.131.20.40");
    Server s4 = new Server("S4", "10.131.20.51");
    Server s5 = new Server("S5", "10.131.10.25");
    
    Server[] nodeInstances = { s1, s2, s3, s4, s5 };
    
    ConsistentHashing ch = new ConsistentHashing();
    for (Server node : nodeInstances) {
      ch.addNode(node);
    }
    
    String[][] entities = { 
        { "1", "Value1" }, 
        { "2", "Value2" }, 
        { "3", "Value3" }, 
        { "4", "Value4" }, 
        { "5", "Value5" }, 
        { "6", "Value6" },
        { "7", "Value7" }, 
        { "8", "Value8" }, 
        { "9", "Value9" }, 
        { "10", "Value10" }, 
        { "11", "Value11" }, 
        { "12", "Value12" }, 
        { "13", "Value13" },
        { "14", "Value114" } 
   };
    
    for (String[] entity : entities) {
      ch.addEntry(entity[0], entity[1]);
    }
    ch.print();
    
    System.out.println("-----------");
    ch.removeNode(s4);
    ch.print();
    
    System.out.println("-----------");
    Server s6 = new Server("S6", "10.10.12.11");
    ch.addNode(s6);
    ch.print();
    
  }
  
}
