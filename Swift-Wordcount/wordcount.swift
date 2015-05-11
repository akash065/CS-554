type file;

app (file out, file log) count_words (file input, file wordcount_script)
{
  bash @wordcount_script @input stdout=@out stderr=@log;
}

app (file out, file log) merge (file f[], file merge_script)
{
  bash @merge_script filenames(f) stdout=@out stderr=@log;
}

file wordcount_script <"wordcount.sh">;
file merge_script     <"merge.sh">;
file chunks[]    <filesys_mapper; location="inputs", suffix="txt">;
file wcounter[];

foreach chunk,i in chunks
{
  file wc_out <single_file_mapper; file=strcat("output/sim_",i,".out")>;
  file wc_log <single_file_mapper; file=strcat("output/sim_",i,".log")>;
  (wc_out, wc_log) = count_words (chunks[i], wordcount_script);
  wcounter[i] = wc_out;
}

file merge_out<"output/final-output.out">;
file merge_log<"output/final-output.log">;
(merge_out, merge_log) = merge(wcounter, merge_script);

