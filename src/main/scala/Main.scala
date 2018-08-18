import org.joda.time.format.DateTimeFormat
import org.joda.time.LocalDate
import org.apache.spark._
import scala.math.BigDecimal

object Main extends App {

 override def main(arg: Array[String]): Unit = {
   var sparkConf = new SparkConf().setMaster("local").setAppName("Transaction")
   var sc = new SparkContext(sparkConf)
   val firstRddRaw = sc.textFile("file:///home/suket/Documents/Transaction Sample data-1.csv")
   val secondRddRaw = sc.textFile("file:///home/suket/Documents/Transaction Sample data-2.csv")
   val firstRdd = firstRddRaw.filter(x => !x.contains("Timestamp"))
   val secondRdd = secondRddRaw.filter(x => !x.contains("Timestamp"))
   val firstTransactionRdd = firstRdd.map(parse1)
   val secondTransactionRdd = secondRdd.map(parse2)
   val TransactionRdd = firstTransactionRdd.union(secondTransactionRdd)
   val creditTransactions = TransactionRdd.filter( _.nature == "C")
   val debitTransactions  = TransactionRdd.filter( _.nature == "D")
   val avgCreditTransactionAmount = BigDecimal(creditTransactions.map(_.amount).reduce ( _ + _ ) / creditTransactions.count).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
   val avgDeditTransactionAmount = BigDecimal(debitTransactions.map(_.amount).reduce ( _ + _ ) / debitTransactions.count).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
   val yearAndCreditTransactions = creditTransactions.map { transaction => (transaction.timestamp.getYear,transaction)}
   val yearwiseCreditTransactions =  yearAndCreditTransactions.groupByKey
   val yearAndDebitTransactions = debitTransactions.map { transaction => (transaction.timestamp.getYear,transaction)}
   val yearwiseDebitTransactions =  yearAndDebitTransactions.groupByKey
   val yearwiseAvgCreditTransactions = yearwiseCreditTransactions.map {case (k,v) => (k,BigDecimal((v.map(_.amount).reduce( _ + _))/v.size).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}.collect.toList
   val yearwiseAvgDebitTransactions = yearwiseDebitTransactions.map {case (k,v) => (k,BigDecimal((v.map(_.amount).reduce( _ + _))/v.size).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}.collect.toList
   val account = TransactionRdd.map { transaction => (transaction.accountId,transaction)}
   val accountWiseTransactions = account.groupByKey
        // (accountId,CB[Transactions])
    val BalanceSheet = accountWiseTransactions.map { case (k,v) => (k,v.map(transactionProcessor).reduce ( _ + _))}.collect.toList
    val highest = BalanceSheet.maxBy(_._2)._1

   println()
   println("Average amount of all Credit Transactions = " +avgCreditTransactionAmount)
   println("Average amount of all Debit Transactions = " +avgDeditTransactionAmount)
   println()
  for ( count <- 0 to yearwiseAvgCreditTransactions.length-1){

   var tuple = yearwiseAvgCreditTransactions(count)
   var year = tuple._1
   var avgCredit = tuple._2
   println("Year = "+year +"    Average amount of all Credit Transactions = "+avgCredit)


  }

  println()
  for ( count <- 0 to yearwiseAvgDebitTransactions.length-1){

   var tuple = yearwiseAvgDebitTransactions(count)
   var year = tuple._1
   var avgDebit = tuple._2
   println("Year = "+year +"    Average amount of all Debit Transactions = "+avgDebit)
  

  }



   println("accountId whose balance amount is highest = "+highest)

   println()
  
   sc.stop()
   
 }

 case class Transaction(accountId: Int, name: String,timestamp: LocalDate, nature: String, amount: Double) extends Serializable with Ordered[Transaction]{
   def compare(that:Transaction) = timestamp.toString().compare(that.timestamp.toString())

 }

  def transactionProcessor(transaction: Transaction):Double = {
    if(transaction.nature == "C")  return transaction.amount
    transaction.amount * -1

  }

  def parse1(row: String): Transaction = {
   val fields = row.split(",")
   val datePattern = DateTimeFormat.forPattern("dd/mm/YYYY")
   val accountId: Int = fields(0).toInt
   val name: String = fields(1)
   val timestamp: LocalDate = datePattern.parseDateTime(fields(2)).toLocalDate()
   val nature: String = fields(3) 
   val amount: Double = fields(4).substring(1).toDouble
   Transaction(accountId,name,timestamp,nature,amount)
   
 }
 
 def parse2(row: String): Transaction = {
   val fields = row.split(",")
   val datePattern = DateTimeFormat.forPattern("dd/mm/YYYY")
   val accountId: Int = fields(0).toInt
   val name: String = fields(1)
   val timestamp: LocalDate = datePattern.parseDateTime(fields(3)).toLocalDate()
   val nature: String = fields(4) 
   val amount: Double = fields(5).substring(1).toDouble
   Transaction(accountId,name,timestamp,nature,amount)
   
 }

}



