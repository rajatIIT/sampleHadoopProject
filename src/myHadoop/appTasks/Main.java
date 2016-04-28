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
 *   
 *   -> The task of creating movies file from the actors list given a set is performed 
 *   	StubMapper and StubReducer given right params. 
 *   -> Use StubMapper and StubReducer to compute the movies list from actor/actress json file. 
 *   	Put this movie list computation logic into two separate classes specific for actors an actresses.
 *  	Write a similar computation logic for genres.
 *   	create a class on top of these two classes which uses these classes depending on the
 *      fixed params to compute the final movie list Mf.
 *      
 *      Remove duplicates from Mf.
 *      
 *         Now execute a frequency run on Mf using StubMapper and StubReducer to compute frequency
 *         or the value list of the optimizing parameter. This list is the result.  
 *  
 * 
 * 	@author rajatpawar
 *
 */
public class Main {
	
	 

}
