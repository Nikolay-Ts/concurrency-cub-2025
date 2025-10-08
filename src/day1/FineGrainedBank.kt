@file:Suppress("DuplicatedCode")

package day1

import day1.Bank.Companion.MAX_AMOUNT
import java.util.concurrent.locks.*
import kotlin.concurrent.withLock

class FineGrainedBank(accountsNumber: Int) : Bank {
    private val accounts: Array<Account> = Array(accountsNumber) { Account() }
    private val locks: Array<ReentrantLock> = Array(accountsNumber) { ReentrantLock() }

    override fun getAmount(id: Int): Long {
        locks[id].withLock {
            val account = accounts[id]
            return account.amount
        }
    }

    override fun deposit(id: Int, amount: Long): Long {
        require(amount > 0) { "Invalid amount: $amount" }
        locks[id].withLock {
            val account = accounts[id]
            check(!(amount > MAX_AMOUNT || account.amount + amount > MAX_AMOUNT)) { "Overflow" }
            account.amount += amount
            return account.amount
        }
    }

    override fun withdraw(id: Int, amount: Long): Long {
        require(amount > 0) { "Invalid amount: $amount" }
        locks[id].withLock {
            val account = accounts[id]
            check(account.amount - amount >= 0) { "Underflow" }
            account.amount -= amount
            return account.amount
        }
    }

    override fun transfer(fromId: Int, toId: Int, amount: Long) {
        require(amount > 0) { "Invalid amount: $amount" }
        require(fromId != toId) { "fromId == toId" }


        locks[fromId].withLock {
            locks[toId].withLock {
                val src = accounts[fromId]
                val dst = accounts[toId]

                check(src.amount >= amount) { "Underflow" }
                check(amount <= MAX_AMOUNT - dst.amount) { "Overflow" }

                src.amount -= amount
                dst.amount += amount
            }
        }
    }

    /**
     * Private account data structure.
     */
    class Account {
        /**
         * Amount of funds in this account.
         */
        var amount: Long = 0

        /**
         * TODO: use this mutex to protect the account data.
         */
        val lock = ReentrantLock()
    }
}