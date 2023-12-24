from Object_Scripts import Part_1, Part_2, Part_3

if __name__ == '__main__':
    # Main Script Structure:
    # - Universal Variables

    # - Part 1 Variables
    # - Part 1 Object creaton
    # - Part 1 methods

    # - Part 2 Variables
    # - Part 2 Object creaton
    # - Part 2 methods

    # - Part 3 Variables
    # - Part 3 Object creaton
    # - Part 3 methods

    permit_name = 'MFSG' # 'MFSG' or 'ARC WDR'

    # File names for the Part 1 script
    first_file_name = f'{permit_name}_data_filtered_ALL_sm_20230711_v1.csv'
    all_filters_file_name = f'{permit_name}_all_filters_ALL_sm_20230711_v1.csv'

    # Permit data names for Part 1 of the script
    permit = '11. CI-5728 MFSG WDR'  ## '11. CI-5728 MFSG WDR'  ## 13. CI-10318 ARC WDR
    permit_data = '221223--MF historical data 2013-2022.csv'# 'ARC WDR historical data.csv' ## '221223--MF historical data 2013-2022.csv'  ## ARC WDR historical data.csv


    part1 = Part_1.Part1()

    part1.set_file_name(first_file_name)
    part1.set_permit(permit)
    part1.set_permit_data(permit_data)
    part1.set_permit_name(permit_name)
    part1.set_all_filters_name(all_filters_file_name)
    print(part1)


    second_file_name = f'{permit_name}_summary_stats_v3.csv'

    part2 = Part_2.Part2()

    part2.set_permit_name(permit_name)
    part2.set_input_name(first_file_name)
    part2.set_output_name(second_file_name)
    print(part2)


    final_csv_name = f'{permit_name}_post_processed_v13.csv'
    third_file_name = f'{permit_name}_post_processed_v13.xlsx'

    part3 = Part_3.Part3()

    part3.set_first_file_dir(all_filters_file_name)
    part3.set_second_file_dir(second_file_name)
    part3.set_initial_final_csv(final_csv_name)
    part3.set_final_output(third_file_name)
    print(part3)