

# load packages:

rm(list=ls())

library(rjson)
library(jsonlite)
library(tidyverse)
library(doParallel)
library(caret)


# read in json files

tip <- stream_in(file("yelp_academic_dataset_tip.json"))
checkin <- stream_in(file("yelp_academic_dataset_checkin.json"))
user <- stream_in(file("yelp_academic_dataset_user.json"))
review <- stream_in(file("yelp_academic_dataset_review.json"))
business <- stream_in(file("yelp_academic_dataset_business.json"))

# create tidy tibbles

business_df<-as_data_frame(cbind(business[,-c(13,15)], business[,13], business[,15]))
user_df<-as_data_frame(user)
review_df<-as_data_frame(review)
tip_df<-as_data_frame(tip)
checkin_df<-as_data_frame(cbind(matrix(checkin$business_id,length(checkin$business_id),1), checkin$time))
names(checkin_df)[1]<-"business_id"

# create count variable for number of checkins to each business
checkin_df$n_checkin<-checkin_df[,-1] %>% as.matrix() %>% apply(1,function(x){sum(na.omit(x))})
  
# create summary variables of reviews for each business
review_df %>% group_by(business_id) %>% 
  summarize(sd_stars=sd(stars), median_stars=median(stars), 
            min_stars=min(stars), max_stars=max(stars), 
            mean_star=mean(stars), n_reviews=n(),
            mean_funny=mean(funny), mean_useful=mean(useful), 
            mean_cool=mean(cool),
            prop1=mean(stars==1))-> review_sum

# create summary variables of tips for each business 
tip_df %>% 
  group_by(business_id) %>% 
  summarize("n_tips"=n()) -> tips_sum

# create dummy variable from categories to determine whether business is a restaurant
strsplit(business_df$categories,",") %>% 
  lapply(trimws) %>% 
  lapply(function(x)sum(str_detect(x, "Restaurants"))) %>%
  unlist() -> business_df$isRestaurant

# create dummy from nested JSON objects
unnest_json<-function(variable, prefix=""){
  variable%>%
    strsplit(",")->aux
    types<-unlist(lapply(strsplit(aux[[(unlist(lapply(aux,length))>1) %>% which() %>% min()]], "'"), function(x)x[2]))
    out<-as_data_frame(matrix(NA,length(variable),length(types)))
    names(out)<-paste(prefix,types, sep="_")
    for(i in seq_along(types)){
      aux %>% lapply(function(x){if(length(x)==1){NA}else{any(rowSums(sapply(c("True",types[i]), grepl, x))==2)}}) %>% 
        unlist() %>% as.numeric()->out[,i]
    }
    return(out)
}


business_df %>% 
  select(-starts_with("Ambience")) %>% 
  bind_cols(unnest_json(business_df$Ambience, "Ambience") )%>% 
  select(-BestNights) %>%
  bind_cols(unnest_json(business_df$BestNights, "BestNights")) %>%
  select(-GoodForMeal) %>%
  bind_cols(unnest_json(business_df$GoodForMeal, "GoodForMeal")) %>%
  select(-Music)  %>%
  bind_cols(unnest_json(business_df$Music, "Music")) %>%
  select(-BusinessParking) %>%
  bind_cols(unnest_json(business_df$BusinessParking, "BusinessParking")) -> business_df


# convert all TRUE & FALSE formatted as strings to Booleans
for(i in 1:ncol(business_df)){
  if(is.character(business_df[,i][[1]])){
    if(nrow(unique(na.omit(business_df[,i])))==2){
      business_df[,i][[1]]<-as.numeric(business_df[,i][[1]])
    }
  }
}


for(i in 1:ncol(business_df)){
  if(is.character(business_df[,i][[1]])){
    if(length(unique(business_df[,i][[1]]))<11){
      business_df[,i][[1]]<-as.factor(business_df[,i][[1]])
    }
  }
}



# Replace Factors with Dummy Variables
find_factors<-names(which(sapply(business_df, is.factor)))

Dummies<-dummyVars(paste("~",paste(find_factors), collapse="+"), 
                   data=business_df, fullRank=TRUE)

business_df %>% 
  select(-find_factors) %>% 
           bind_cols(as_data_frame(predict(Dummies, newdata=business_df)))-> business_df

# select all restaurants
business_df %>% 
  filter(isRestaurant==1) -> business_df

# remove variables that are missing for all restaurants
business_df<-business_df[,-which((round(apply(business_df,2,function(x)mean(is.na(x))), digits=2) ==1))]


# to analyze impact of missing values show proportion of missing values 
# and effect on outcome variable
selector<-which((apply(business_df,2,function(x)mean(is.na(x)))>0.05))
out<-as_data_frame(matrix(NA,length(selector),4))
names(out)<-c("Variable","Prop","NonMissing","Missing")
out$Variable<-names(selector)
out$Prop<-apply(business_df[,selector],2,function(x)mean(is.na(x)))
for(i in seq_along(selector)){
  out[i,3]<-mean(business_df$is_open[which(is.na(business_df[,selector[i]])==FALSE)])
  out[i,4]<-mean(business_df$is_open[which(is.na(business_df[,selector[i]]))])
}
out$Diff<-out$NonMissing-out$Missing
out %>% arrange(desc(Diff)) %>% print(n=Inf)

# it can be seen that missing variables with higher importance have lower proportion of missingness
# discard variables with more than 30 percent missingness and impute the rest

business_df<-business_df[,-which((apply(business_df,2,function(x)mean(is.na(x)))>0.3))]


#############################

# combine datasets


business_df %>% select(-latitude, -longitude, 
                       -name, -neighborhood, -address, -city,
                       - state, -postal_code, -stars, -categories,
                       -Tuesday, -Wednesday, -Thursday, -Friday, -Saturday,
                       -isRestaurant) %>% 
  dplyr::left_join(review_sum, by="business_id") %>%
  dplyr::left_join(tips_sum, by="business_id") %>%
  dplyr::left_join(checkin_df %>% 
  select(c("business_id","n_checkin")), by="business_id") -> full_data


# split data set in training and test set
set.seed(123)
trainIndex <- createDataPartition(full_data$is_open, list=FALSE, p = .8)
train<-full_data[trainIndex,]

train$is_open<-as.factor(train$is_open)
levels(train$is_open)[levels(train$is_open)=="1"] <- "open"
levels(train$is_open)[levels(train$is_open)=="0"] <- "closed"
test<-full_data[-trainIndex,]

test$is_open<-as.factor(test$is_open)
levels(test$is_open)[levels(test$is_open)=="1"] <- "open"
levels(test$is_open)[levels(test$is_open)=="0"] <- "closed"

train<-train[1:5000,]

train<-train %>% select(-business_id, -n_reviews)

# impute missing values by median values. USE MORE SOPHISTICATED APPROACH IN UPDATED VERSION
impute<-preProcess(dplyr::select(train, -is_open, -mean_star), method="medianImpute")
train<-predict(impute, newdata=train)
test<-predict(impute, newdata=test)


##############################          Fit Models          #########################################


fit<-glm(is_open~., data=train, family="binomial")

library(margins)
mar<-margins(fit)
summary(fit)

control <- trainControl(method="none", #number=10, repeats=1,
                        summaryFunction=twoClassSummary, 
                        classProbs=TRUE,
                        savePredictions = TRUE)

apply(train,2,function(x)sum(is.na(x)))

## register parallel backend!!!

#cl <- makePSOCKcluster(10)
#registerDoParallel(cl)
#registerDoSEQ()
logit <-caret::train(is_open~., data=train, method="glm", family="binomial", trControl=control)
backw <-caret::train(is_open~., data=train, method="glmStepAIC", trControl=control, trace=FALSE)
rf<- caret::train(is_open~., data=train, method="rf", metric="ROC", trControl=control)
xgb_lin<- caret::train(is_open~., data=train, method="xgbLinear", metric="ROC", trControl=control)
#xgb_tree<- caret::train(is_open~., data=train, method="xgbTree", metric="ROC", trControl=control)
#stopCluster(cl)

summary(logit)
summary(backw)

aux<-varImp(rf$finalModel)
Imp<-tibble::as_tibble(list("Variable"=row.names(aux),
                            "Importance"=aux$Overall)) %>% 
  dplyr::arrange(desc(Importance)) %>%
  dplyr::filter(Importance>20)

ggplot(Imp, aes(x=reorder(Variable,Importance),y=Importance, fill=Importance))+
  geom_bar(stat="identity")+
  coord_flip()
