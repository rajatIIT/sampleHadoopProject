package myHadoop.appTasks;

/**
 * 
 * 	The main class that will coordinate all the tasks.
 * 
 *  Tasks : 
 *  given the set of movies, get the frequencies of various actress involved in the movies.
 *  given a set of actors/actresses get the set of movies they are involved with.
 *  given a set of genre, get a list of movies that confer to the genre and
 *  given a set of movies find the frequencies of various genres.
 *  
 *   
 *   Standard app tasks : 
 *   
 *   Actress best task : Given a set of actors, a set of genres get the actresses that have 
 *   performed most FREQUENTLY : 
 *   
 *   1) Calculate Ma : The set of movies that correspond to the list of actors. 
 *   2) Calculate Mg : The set of movies that correspond to the list of genres 
 *   3) Mr = Ma intersection Mg.
 *   	-> Perform union on the two lists.
 *   	-> Execute a mapreduce job on the union counting the number of common lines. 
 *   	-> Use output of this job as the "union without duplicates". 
 *   
 *   4) Now execute a frequency run on Mr.
 *   
 *   Task : static list intersection.
 * 
 * 	@author rajatpawar
 *
 */
public class Main {
	
	 

}
