package org.aksw.linkedspending.scripts;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringEscapeUtils;
import com.google.common.base.Optional;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.i18n.LdLocale;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.*;

/** Reads labels from an LinkedSpending ntriples file containing only the labels without language tags and generates the labels with language tags.
 * Outputs ntriples.
 * If needed, I can also add TSV with no escape chars (tabs get replaced by spaces in the label) with the URL in the first column, the label in the second column
 * and the language tag in the third column. */

public class AddLanguageTags
{
	// needs sorted input file, will group detection for a dataset
	private static final boolean accumulate = true;
	private static final String	INSTANCE	= "http://linkedspending.aksw.org/instance";
	private static final String	ONTOLOGY	= "http://linkedspending.aksw.org/ontology/";
	private static final int	MAX_ACCUMULATE	= 5_000;

	public static void main(String[] args) throws FileNotFoundException, IOException
	{
		List<LanguageProfile> languageProfiles = new LanguageProfileReader().readAllBuiltIn();

		//build language detector:
		LanguageDetector languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard())
				.withProfiles(languageProfiles)
				.build();

		//create a text object factory
		//		TextObjectFactory textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText();
		TextObjectFactory textObjectFactory = accumulate?
				CommonTextObjectFactories.forDetectingOnLargeText():CommonTextObjectFactories.forDetectingShortCleanText();

				File inFile = new File(args.length>0?args[0]:"langdetect/labels.nt");
				Pattern labelPattern = Pattern.compile("\"([^\"]*)\"");
				Pattern urlPattern = Pattern.compile("^<([^>]*)>");
				//		http://linkedspending.aksw.org/ontology/gifu_kessan_2011_212148-amount-spec>

				File outFolder = new File("langdetect/"+System.currentTimeMillis()/1000);
				outFolder.mkdirs();
				String accumulateString = accumulate?"-accumulated":"";
				Map<String,String> accumulatedLabels = new HashMap<String,String>();

				try (BufferedReader in = new BufferedReader(new FileReader(inFile)))
				{
					try(PrintWriter outOnlyTagged = new PrintWriter(new File(outFolder,"onlytagged"+accumulateString+".nt")))
					{
						//			try(PrintWriter outAll = new PrintWriter("langdetect/langdetect-all"+System.currentTimeMillis()/1000+".nt"))
						{
							String line;
							String lastIdentifier = null;
							int count = 0;
							while ((line = in.readLine()) != null)
							{
								Matcher m = labelPattern.matcher(line);
								if(m.find())
								{
									count++;
									// labels contain escaped unicode (\u1234)
									// there is no pure unicode unescape available so use java unescape and hope for the best
									String label = m.group(1);
									String unescapedLabel = StringEscapeUtils.unescapeJava(label);
									line = line.replace(label, unescapedLabel);
									if(accumulate)
									{
										Matcher urlMatcher = urlPattern.matcher(line);
										urlMatcher.find();
										String url = urlMatcher.group(1);
										String identifier = null;
										if(url.startsWith(INSTANCE))
										{
											// we can identify dataset name here so use that
											String rest = url.substring(INSTANCE.length()+1); // remove the slash as well
											int index = rest.contains("/")?rest.indexOf('/'):rest.length();
											identifier = rest.substring(0, index);
										} else if(url.startsWith(ONTOLOGY))
											// we can identify dataset name here also so use that
											// uris under ontology dont have / signs but some are "XYZ-spec"
										{
											String rest = url.substring(ONTOLOGY.length());
											identifier = rest.replace("spec","");
										} else // we don't really know the structure but removing the last part is a good guess
										{
											identifier = url.substring(0,url.lastIndexOf('/'));
										}
										// TODO: write out at the end as well, isn't a priority as losing one or two labels doesn't matter for our experiment
										if((!identifier.equals(lastIdentifier))||accumulatedLabels.size()>MAX_ACCUMULATE)
										{
											if(!accumulatedLabels.isEmpty())
											{
												String corpus = accumulatedLabels.values().stream().reduce((a,b)->a+" "+b).get();
												TextObject textObject = textObjectFactory.forText(corpus);
												Optional<LdLocale> lang = languageDetector.detect(textObject);
												// write out old labels
												{
													if(lang.isPresent())
													{
														String tag = lang.get().getLanguage();
														for(String key: accumulatedLabels.keySet())
														{
															outOnlyTagged.println("<"+key+"> <http://www.w3.org/2000/01/rdf-schema#label> \""
																	+accumulatedLabels.get(key)+"\"@"+tag+" .");
														}

													}
//													else
//													{
//														for(String key: accumulatedLabels.keySet())
//														{
//															outOnlyTagged.println("<"+key+"> <http://www.w3.org/2000/01/rdf-schema#label> \""
//																	+accumulatedLabels.get(key)+"\" .");
//
//														}
//													}
												}
												accumulatedLabels.clear();
											}
										}
										accumulatedLabels.put(url, unescapedLabel);
										lastIdentifier = identifier;
									} else // not accumulate
									{
										//						System.out.println(label);
										TextObject textObject = textObjectFactory.forText(unescapedLabel);
										Optional<LdLocale> lang = languageDetector.detect(textObject);
										if(lang.isPresent())
										{
											String tag = lang.get().getLanguage();
											String taggedLine = line.replace("\" .", "\"@"+tag+" .");
											if(line.equals(taggedLine)) System.err.println("tag could not be added to line "+line);
											//							outAll.println(taggedLine);
											outOnlyTagged.println(taggedLine);
										} else
										{
											//							outAll.println(line);
										}
									}
								}
								else
								{
									System.err.println("could not find the label in the string: '"+line+"'");
									//						outAll.println(line);
								}

								if(count%10000==0) System.out.println("Processed "+count+" labels.");

							}
							System.out.println("Processed "+count+" labels.");
						}
					}
				}
	}

}