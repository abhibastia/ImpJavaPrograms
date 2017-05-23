
/***
 Draw a staircase of height N in the format given below.
For example:
     #
    ##
   ###
  ####
 #####
######

  **/


package misc;

import java.util.Scanner;

public class Staircase {

	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
        int num  = Integer.parseInt(sc.nextLine());
        for(int j=0;j<num;j++){
            for(int i=1;i<=num;i++){
                System.out.print(i<num-j?" ":"#");
            }
            System.out.println("");
        }
        sc.close();

	}

}
