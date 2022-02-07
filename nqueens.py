import random

def succ(state, static_x, static_y):
    valid = [] #list of valid successors i.e. has a queen on the static coordinate

    if state[static_x] != static_y: #returns empty list if initial_state does not contain static coordinate
        return []

    for i in range(len(state)):
        this_state = state.copy()
        if i == static_x: #does not change static coordinate
           placeholder = 1 
        else:
            if this_state[i] < len(this_state) - 1: #explore states at each y ++1, --1
                this_state[i] += 1
                valid.append(this_state)
            this_state = state.copy()
            if this_state[i] > 0:
                this_state[i] -= 1
                valid.append(this_state)

    return sorted(valid) #return sorted list of valid states


def f(state):
    counter = 0 #counter for f value
    for i in range(len(state)):
        vertical = state[i]
        for j in range(len(state)):
            if i - j > 0 and vertical == state[j] + i - j: #conditional statements for checking for attacking queens behind the current queen
                counter += 1
                break
            elif i - j > 0 and vertical == state[j] - i + j: 
                counter += 1
                break
            elif i - j > 0 and vertical == state[j]: #check for queens along same x axis
                counter += 1
                break
            else:
                if i - j < 0:
                    if j < len(state) and vertical == state[j]: #conditional statemnts for checking for attacking queens ahead of the current queen
                        counter += 1
                        break
                    elif j < len(state) and vertical == state[j] + j - i:
                        counter += 1
                        break
                    elif j < len(state) and vertical == state[j] - j + i:
                        counter += 1
                        break
                
    return counter


def choose_next(curr, static_x, static_y): #chooses next best successor by calculating f values
    another_state = curr.copy()
    if another_state[static_x] != static_y:
        return
    list_f = []
    list_f.append([f(curr), curr]) #add lists of lists containing f value and list of coordinates
    succ_states = succ(another_state, static_x, static_y)

    for i in range(len(succ_states)):
        temp = f(succ_states[i])
        if succ_states[i][static_x] == static_y: #checks that static point is intact
            list_f.append([temp, succ_states[i]])
    list_f1 = sorted(list_f) #sorts list by f value, same f values get sorted numerically by their associated lists
    return list_f1[0][1] #return the best successor state


def n_queens(initial_state, static_x, static_y): #preform hill climbing algorithm until either a local min is found or until a configuration where f = 0 is found
    state1 = initial_state.copy()
    state2 = choose_next(state1, static_x, static_y) #keep copy of previous and current states
    evaluate = f(state1)
    prev_evaluate = 1
    print(state1, end='')
    print(' - f=' + str(evaluate))
    while evaluate != 0:
        state1 = state2.copy()
        state2 = choose_next(state1, static_x, static_y)
        prev_evaluate = f(state1)
        evaluate = f(state2)
        print(state1, end='')
        print(' - f=' + str(prev_evaluate))
        if prev_evaluate == evaluate or evaluate == 0: #check for either 2 of the same f scores in a row or an f score of 0
            print(state2, end='')
            print(' - f=' + str(evaluate))
            break
    return state2


def n_queens_restart(n, k, static_x, static_y):
    random.seed(1) # set seed
    list = []
    
    for l in range(n): #create random nxn board with n queens
        list.append(random.randint(0, n-1))

    for m in range(k):
        list[static_x] = static_y #set the static point
        x = n_queens(list, static_x, static_y) #run hill climb algorithm
        if f(x) == 0: #if solution is found, print and end
            print(x, end='')
            print(' - f=' + str(f(x)))
            break
        elif m == k: #if we get to the kth iteration, print the best solution and its possible successors with the same k values
            y = succ(x, static_x, static_y)
            for i in range(len(y)):
                print(y[i], end='')
                print(' - f=' + str(f(y[i])))
        else: #else, generate a new random list and try again, up to k times
            list = []
            for l in range(n):
                list.append(random.randint(0, n-1))
