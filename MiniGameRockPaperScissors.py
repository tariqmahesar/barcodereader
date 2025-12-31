import random

user_score = 0
computer_score = 0

choices = ['rock', 'paper', 'scissors']

def get_computer_choice():
    return random.choice(choices)

def determine_winner(user, computer):
    if user == computer:
        return "tie"
    elif (user == "rock" and computer =="scissors")or \
         (user == "paper" and computer == "rock") or \
         (user == "scissors" and computer == "paper"):
        return "user"
    else:
        return "computer"

while True:
    user_choice = input("\nChoose: rock, paper, or scissors: ").lower()

    if user_choice == "quit":
        break
    elif user_choice not in choices:
        continue

    computer_choice = get_computer_choice()
    print("Computer choice:", computer_choice)

    result = determine_winner(user_choice, computer_choice)

    if result == "user":
        print("You won! 🎉")
        user_score  += 1
    elif result == "computer":
        print("Computer Win💻")
        computer_score += 1
    else:
        print("it's tie🤝")

    print(f"Score - you {user_score} | Computer: {computer_score} ")



