"""
 Naive-Bayes
 
 Description: Implementation of the Naive-Bayes classifier method.
 
 Args [required]: (number of samples) (split % as a decimal)
"""

import pandas as pd
import numpy as np
import sys
import os
import copy
from tqdm import tqdm
import warnings

# ignore warnings
warnings.filterwarnings('ignore')



def avc_service(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]
    
    values = np.sort(pd.unique(sample.service.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['service'] == i])
        n_count = len(sample_n[sample_n['service'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_sig(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.signature.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['signature'] == i])
        n_count = len(sample_n[sample_n['signature'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_zipcode(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.zipcode.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['zipcode'] == i])
        n_count = len(sample_n[sample_n['zipcode'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_provider(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.provider.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['provider'] == i])
        n_count = len(sample_n[sample_n['provider'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_area(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.area.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['area'] == i])
        n_count = len(sample_n[sample_n['area'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_days(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.days.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['days'] == i])
        n_count = len(sample_n[sample_n['days'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    for i in avc[4:].itertuples():
        avc.loc[4, True] += i._1
        avc.loc[4, False] += i._2
    
    avc = avc.drop(index=avc[4:].index)
    
    return avc
    
    
    
    
def avc_delays(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.delays.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['delays'] == i])
        n_count = len(sample_n[sample_n['delays'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    for i in avc[3:].itertuples():
        avc.loc[3, True] += i._1
        avc.loc[3, False] += i._2
    
    avc = avc.drop(index=avc[4:].index)
    
    return avc
    
    
    
    
def avc_failures(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.failures.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['failures'] == i])
        n_count = len(sample_n[sample_n['failures'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    for i in avc[2:].itertuples():
        avc.loc[2, True] += i._1
        avc.loc[2, False] += i._2
    
    avc = avc.drop(index=avc[3:].index)
    
    return avc
    
    
    
    
def avc_address(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.address.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['address'] == i])
        n_count = len(sample_n[sample_n['address'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_res(sample):
    sample_d = sample[sample['delivered'] == True]
    sample_n = sample[sample['delivered'] == False]

    values = np.sort(pd.unique(sample.resolution.values))
    avc = pd.DataFrame(index=values, columns=[True, False])
    avc = avc.fillna(0)
    
    for i in values:
        d_count = len(sample_d[sample_d['resolution'] == i])
        n_count = len(sample_n[sample_n['resolution'] == i])
        avc.loc[i, True] = d_count
        avc.loc[i, False] = n_count
        
    return avc
    
    
    
    
def avc_vol(sample):
    avc = pd.DataFrame(index=['<20k', '20k-30k', '30k+'], columns=[True, False])
    avc = avc.fillna(0)
    
    for i in sample.itertuples():
        volume = i.volume
        class_v = i.delivered
        
        if volume < 20000:
            avc.loc['<20k', class_v] += 1
            
        elif volume >= 20000 and volume < 29999:
            avc.loc['20k-30k', class_v] += 1
            
        else:
            avc.loc['30k+', class_v] += 1
            
    return avc
    
    
    
    
def avc_precip(sample):
    avc = pd.DataFrame(index=['0','1-3', '3+'], columns=[True, False])
    avc = avc.fillna(0)
    
    for i in sample.itertuples():
        precip = i.precip
        class_v = i.delivered
        
        if precip >= 0.0 and precip < 1.0:
            avc.loc['0', class_v] += 1
            
        elif precip >= 1.0 and precip < 3.0:
            avc.loc['1-3', class_v] += 1
            
        else:
            avc.loc['3+', class_v] += 1
            
    return avc
    
    
    
    
def avc_temp(sample):
    avc = pd.DataFrame(index=['<30','30-50', '50+'], columns=[True, False])
    avc = avc.fillna(0)
    
    for i in sample.itertuples():
        temp = i.temp
        class_v = i.delivered
        
        if temp < 30:
            avc.loc['<30', class_v] += 1
            
        elif temp >= 30 and temp < 50:
            avc.loc['30-50', class_v] += 1
            
        else:
            avc.loc['50+', class_v] += 1
            
    return avc




def build_avc(train_sets):
    # dictionary to hold our avc sets in (per training set)
    avc_sets = {}
    
    # build tables for every training set
    print("Building AVC tables...")
    pbar = tqdm(range(len(train_sets)))
    for x in pbar:
        pbar.set_description("Model " + str(x))
        # get our sample set, divide into classes
        sample = train_sets[x]
        
        sample_d = sample[sample['delivered'] == True]
        sample_n = sample[sample['delivered'] == False]
        
        # dictionary to hold AVC tables
        avc_tables = {}
        
        # build service table
        avc_tables['service'] = avc_service(sample)
        
        # build signature table
        avc_tables['signature'] = avc_sig(sample)
        
        # build zipcode table
        avc_tables['zipcode'] = avc_zipcode(sample)
        
        # build provider table
        avc_tables['provider'] = avc_provider(sample)
        
        # build area table
        avc_tables['area'] = avc_area(sample)
        
        # build days table
        avc_tables['days'] = avc_days(sample)
        
        # build delays table
        avc_tables['delays'] = avc_delays(sample)
        
        # build failures table
        avc_tables['failures'] = avc_failures(sample)
        
        # build address table
        avc_tables['address'] = avc_address(sample)
        
        # build resolution table
        avc_tables['resolution'] = avc_res(sample)
        
        # build volume table
        avc_tables['volume'] = avc_vol(sample)
        
        # build precip table
        avc_tables['precip'] = avc_precip(sample)
        
        # build temp table
        avc_tables['temp'] = avc_temp(sample)
        
        # apply Laplacian correction  
        for i in avc_tables:
            avc_tables[i] += 1
    
        # add sample AVC tables to dictionary
        avc_sets[x] = avc_tables
        
    return avc_sets




def split(sample_list, p):
    # dataframe to hold all of our testing samples
    test_samples = pd.DataFrame(columns=sample_list[0].columns)
    
    # dictionary to hold our training sets
    train_sets = {}
    
    # loop over all the sets to obtain training sets and testing samples
    for i in range(len(sample_list)):
        # get the sample set from our list
        sample_set = sample_list[i]
        
        # get the spliting index
        split_index = int(len(sample_set) * (1 - p))
        
        # do the train/test spliting
        train = sample_set.iloc[:split_index]
        test = sample_set.iloc[split_index:].reset_index(drop=True)
        
        # add our training set to the dictionary
        train_sets[i] = train
        
        # append test samples to our sample dataframe
        test_samples = pd.concat([test_samples, test])
        
    # reset the index for our test sample dataframe
    test_samples = test_samples.reset_index(drop=True)
        
    return train_sets, test_samples
    
    
    
    
def model_test(avc_sets, test_samples):
    conf_matrices = {}
    
    for x in range(len(avc_sets)):
        # get testing set and AVC tables
        avc_tables = avc_sets[x]
        
        # create a confusion matrix for the set
        conf_matrix = pd.DataFrame(index=[True, False], columns=[True, False])
        conf_matrix = conf_matrix.fillna(0)
        
        print("Testing for Model:", x)
        for obj in tqdm(test_samples.itertuples()):
            # list to hold our probabilities
            d_probs = []
            n_probs = []
            
            # compute service probability
            value = obj.service
            
            avc = avc_tables['service']
            
            prob_d = avc.loc[value, True]/avc[True].sum()
            prob_n = avc.loc[value, False]/avc[False].sum()
            
            d_probs.append(prob_d)
            n_probs.append(prob_n)
            
            # compute signature probability       
            value = obj.signature
            
            avc = avc_tables['signature']
            
            prob_d = avc.loc[value, True]/avc[True].sum()
            prob_n = avc.loc[value, False]/avc[False].sum()
            
            d_probs.append(prob_d)
            n_probs.append(prob_n)
            
            # compute zipcode probability
            value = obj.zipcode
            
            avc = avc_tables['zipcode']
            
            try:
                prob_d = avc.loc[value, True]/avc[True].sum()
                prob_n = avc.loc[value, False]/avc[False].sum()
            except:
                prob_d = 1
                prob_n = 1
                
                d_probs.append(prob_d)
                n_probs.append(prob_n)
            
            # compute provider probability
            value = obj.provider
            
            avc = avc_tables['provider']
            
            prob_d = avc.loc[value, True]/avc[True].sum()
            prob_n = avc.loc[value, False]/avc[False].sum()
            
            d_probs.append(prob_d)
            n_probs.append(prob_n)
            
            # compute area probability
            value = obj.area
            
            avc = avc_tables['area']
            
            try:
                prob_d = avc.loc[value, True]/avc[True].sum()
                prob_n = avc.loc[value, False]/avc[False].sum()
            except:
                prob_d = 1
                prob_n = 1
            
            d_probs.append(prob_d)
            n_probs.append(prob_n)
            
            # compute days probability
            value = obj.days
            if value > 4:
                value = 4
                
            avc = avc_tables['days']
            
            prob_d = avc.loc[value, True]/avc[True].sum()
            prob_n = avc.loc[value, False]/avc[False].sum()
            
            d_probs.append(prob_d)
            n_probs.append(prob_n)
            
            # compute delays probability
            value = obj.delays
            if value > 3:
                value = 3
                
            avc = avc_tables['delays']
            
            prob_d = avc.loc[value, True]/avc[True].sum()
            prob_n = avc.loc[value, False]/avc[False].sum()
            
            d_probs.append(prob_d)
            n_probs.append(prob_n)
            
            # compute failures probability
            value = obj.failures
            if value > 2:
                value = 2
                
            avc = avc_tables['failures']
            
            prob_d = avc.loc[value, True]/avc[True].sum()
            prob_n = avc.loc[value, False]/avc[False].sum()
            
            d_probs.append(prob_d)
            n_probs.append(prob_n)
            
            # compute address probability
            value = obj.address
            
            avc = avc_tables['address']
            
            prob_d = avc.loc[value, True]/avc[True].sum()
            prob_n = avc.loc[value, False]/avc[False].sum()
            
            d_probs.append(prob_d)
            n_probs.append(prob_n)
            
            # compute resolution probability
            value = obj.resolution
            
            avc = avc_tables['resolution']
            
            prob_d = avc.loc[value, True]/avc[True].sum()
            prob_n = avc.loc[value, False]/avc[False].sum()
            
            d_probs.append(prob_d)
            n_probs.append(prob_n)
            
            # compute volume probability
            value = obj.volume
            if value < 20000:
                value = '<20k'
            elif value >= 20000 and value < 29999:
                value = '20k-30k'
            else:
                value = '30k+'
                
            avc = avc_tables['volume']
            
            prob_d = avc.loc[value, True]/avc[True].sum()
            prob_n = avc.loc[value, False]/avc[False].sum()
            
            d_probs.append(prob_d)
            n_probs.append(prob_n)
            
            # compute precip probability
            value = obj.precip
            if value < 1.0:
                value = '0'
            elif value >= 1.0 and value < 3.0:
                value = '1-3'
            else:
                value = '3+'
                
            avc = avc_tables['precip']
            
            prob_d = avc.loc[value, True]/avc[True].sum()
            prob_n = avc.loc[value, False]/avc[False].sum()
            
            d_probs.append(prob_d)
            n_probs.append(prob_n)
            
            # compute temp probability
            value = obj.temp
            if value < 30:
                value = '<30'
            elif value >= 30 and value < 50:
                value = '30-50'
            else:
                value = '50+'
                
            avc = avc_tables['temp']
            
            prob_d = avc.loc[value, True]/avc[True].sum()
            prob_n = avc.loc[value, False]/avc[False].sum()
            
            d_probs.append(prob_d)
            n_probs.append(prob_n)
            
            # compute total probability for each class and compare
            prob_d = np.prod(d_probs)
            prob_n = np.prod(n_probs)
            
            # determine the class label
            pred_class = False
            if prob_d > prob_n:
                pred_class = True
                
            # add prediction to the confusion matrix
            conf_matrix.loc[obj.delivered, pred_class] += 1
            
        # once completed, save the confusion matrix to our dictionary
        conf_matrices[x] = conf_matrix
        
    return conf_matrices




def results(conf_matrices, train_sets):
    best_model = 0
    best_score = 0
    best_stats = []
    for x in range(len(conf_matrices)):    
        # get confusion matrix for the model
        conf_matrix = conf_matrices[x]
        
        # get the training set for the model
        train = train_sets[x]
        
        # get number of samples for each class
        p = len(train[train['delivered'] == True])
        n = len(train[train['delivered'] == False])
        
        # get classification metrics from confusion matrix
        tp = conf_matrix.loc[True, True]
        tn = conf_matrix.loc[False, False]
        fn = conf_matrix.loc[True, False]
        fp = conf_matrix.loc[False, True]
        
        # calculate skew
        a = p/(p+n)
        
        # calculate recall (true positive rate)
        r = tp/(tp+fn)
        
        # calculate precision
        p = tp/(tp+fp)
        
        # calculate F1 score
        f1 = (2*r*p)/(r+p)
        
        # calculate accuracy
        acc = (tp+tn)/(tp+tn+fn+fp)
        
        # statistic voting
        if len(best_stats) == 0:
            best_model = x
            best_stats = [a, r, p, f1, acc]
            best_score = f1
        else:
            if f1 > best_score:
                best_model = x
                best_stats = [a, r, p, f1, acc]
                best_score = f1
        
    # print results for best model
    print("Best Model:", best_model)
    print("----------------")
    print('Recall:', round(best_stats[2], 3))
    print('Precision:', round(best_stats[1], 3))
    print('F1 Score:', round(best_stats[3], 3))
    print('Accuracy:', round(best_stats[4], 3))
    print('Skew:', round(best_stats[0], 3))
    print()
    print("Confusion Matrix:")
    print(conf_matrices[best_model])
    print('\n\n')
    
    


def main(args):
    # check if path for weather data exists
    path = 'samples/'
    if not os.path.exists(path):
        os.makedirs(path)
        print("No sample directory found.")
        print("Place generated sample sets in \'samples/\'.")
        print("You may need to generate your samples first with sampler.py.")
        input("Press enter to continue...")
        sys.exit()
     
    # check if number of sample sets were passed
    if len(args) != 3:
        print("Arguments are not set right.")
        print("Run script as follows:")
        print("python naive_bayes.py [number of samples] [split % as a decimal]")
        input("Press enter to continue...")
        sys.exit()
        
    # filenames to look for
    filename = path + 'original_sampleX.csv'
    
    # number of sample sets
    num_sets = int(args[1])
    
    # percentage of train/test split
    split_percent = float(args[2])
    
    # list to contain all the sample sets
    sample_list = []
    
    
    # load the sample sets if they exists
    try:
        for x in range(num_sets):
            s = copy.deepcopy(filename)
            s = s.replace('X', str(x))
            sample_list.append(pd.read_csv(s))
    except:
        print("No data found.")
        print("Place generated sample sets in \'samples/\'.")
        input("Press enter to continue...")
        sys.exit()

    # format attributes volume, precip, and temp
    for i in sample_list:
        i['volume'] = i['volume'].astype('int')
        i['temp'] = i['temp'].astype('int')
        i['precip'] = np.round(i['precip'], decimals=2)
    
    # split our sample sets to have training sets and one testing set
    train_sets, test_samples = split(sample_list, split_percent)

    # build AVC tables for our training sets
    avc_sets = build_avc(train_sets)
    
    # test our different AVC sets with our testing samples
    conf_matrices = model_test(avc_sets, test_samples)
    
    # calculate and print results
    results(conf_matrices, train_sets)


if __name__ == "__main__":
    main(sys.argv)