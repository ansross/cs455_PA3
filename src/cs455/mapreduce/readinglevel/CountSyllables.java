package cs455.mapreduce.readinglevel;

public class CountSyllables {
	public static int countSyllables(String word){
		char[] vowels = { 'a', 'e', 'i', 'o', 'u', 'y' };
		String currentWord = word;
		int numVowels = 0;
		boolean lastWasVowel = false;
		for (int i=0; i<currentWord.length(); ++i)
		{
			char wc = currentWord.charAt(i);
			boolean foundVowel = false;
			for(char v : vowels)
			{
				//don't count diphthongs
				if (v == wc && lastWasVowel)
				{
					foundVowel = true;
					lastWasVowel = true;
					break;
				}
				else if (v == wc && !lastWasVowel)
				{
					numVowels++;
					foundVowel = true;
					lastWasVowel = true;
					break;
				}
			}

			//if full cycle and no vowel found, set lastWasVowel to false;
			if (!foundVowel)
				lastWasVowel = false;
		}
		//remove es, it's _usually? silent
		if (currentWord.length() > 2 && 
		currentWord.substring(currentWord.length() - 2) == "es")
			numVowels--;
		// remove silent e
		else if (currentWord.length() > 1 &&
				currentWord.substring(currentWord.length() - 1) == "e")
			numVowels--;

		return numVowels;
	}
}
