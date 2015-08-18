package org.aksw.linkedspending.scripts;

import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.*;

public class EstimateDataSetLanguages
{

	public static void main(String[] args) throws FileNotFoundException, IOException
	{
		List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();

		LanguageDetector languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
				.withProfiles(languageProfiles)
				.build();

		TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingShortCleanText();
		new File("langdetect").mkdir();
		try(PrintWriter out = new PrintWriter("langdetect/percentages"+System.currentTimeMillis()/1000))
		{
			for(File dataset : new File("output").listFiles(f->f.getName().endsWith(".nt")))
			{
				int labelCount = 0;
				AtomicInteger englishCount = new AtomicInteger();
				try (BufferedReader in = new BufferedReader(new FileReader(dataset)))
				{
					for(String line = in.readLine(); line!=null; line = in.readLine())
					{
						if(!line.contains(RDFS.label.getURI())) {continue;}
						labelCount++;
						AddLanguageTags.getLabel(line).ifPresent(label->
						{
							TextObject textObject = textObjectFactory.forText(label);
							com.google.common.base.Optional<LdLocale> lang = languageDetector.detect(textObject);
							if(lang.isPresent())
							{
								String tag = lang.get().getLanguage();
								if(tag.equals("en")) {englishCount.incrementAndGet();}
							}
						});
					}
				}
				String output = (double)englishCount.get()/labelCount+"	"+englishCount+"	"+labelCount+"	"+dataset.getName();
				out.println(output);
				System.out.println(output);
			}
		}
	}

}