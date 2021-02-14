import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator, MultipleLocator, FuncFormatter
import seaborn as sns

from scipy import stats



def plot_variable_pairs(df):
    """
    Takes:
          df
    Returns:
          PairGrid plot of all relationships
          histogram and scatter plots
    """
    g=sns.PairGrid(df)
    g.map_diag(plt.hist)
    g.map_offdiag(plt.scatter)
    plt.show()


def months_to_years(df):
    """
    Takes:
          df
    Returns:
          df with new feature "tenure_years"
    """
    df["tenure_years"] = round(df.tenure // 12).astype(object)
    return df


def plot_categorical_and_continuous_vars(categorical_var, continuous_var, df):
    """
    Takes:
          df
    Returns:
          three plots of categorical var with continuous var
    """
    plt.suptitle(f'{continuous_var} by {categorical_var}', fontsize=18)
    
    sns.lineplot(x=categorical_var, y=continuous_var, data=df)
    plt.xlabel(categorical_var, fontsize=12)
    plt.ylabel(continuous_var, fontsize=12)
    plt.show()
    
    
    sns.catplot(x=categorical_var, y=continuous_var, data=df, kind="swarm", palette='Blues')
    plt.xlabel(categorical_var, fontsize=12)
    plt.ylabel(continuous_var, fontsize=12)
    plt.show()
    
    sns.catplot(x=categorical_var, y=continuous_var, data=df, kind="bar", palette='Purples')
    plt.xlabel(categorical_var, fontsize=12)
    plt.ylabel(continuous_var, fontsize=12)
    plt.show()


def plot_categorical_and_continuous_vars_telco(df):
    """
    Takes: 
        telco df
    Returns:
        three plots comparing tenure_years to total_charges
    """
    fig, (ax1, ax2, ax3) = plt.subplots(figsize=(12,10), nrows=3,ncols=1, sharex=True)
    plt.style.use('seaborn-bright')

    plt.suptitle('Total Charges by Tenure Years', fontsize=18)

    ax1.plot(df.tenure_years, df.total_charges, color='mediumblue')
    ax1.set_ylabel('US Dollars', fontsize=14)

    ax2.bar(df.tenure_years, df.total_charges, color='dodgerblue')
    ax2.set_ylabel('US Dollars', fontsize=14)

    ax3.scatter(df.tenure_years, df.total_charges, color='skyblue')
    ax3.set_xlabel('Tenure in Years', fontsize=14)
    ax3.set_ylabel('US Dollars', fontsize=14)

    plt.tight_layout()
    plt.show()


def telco_pie(df):
    plt.style.use('seaborn-paper')
    labels = ['0 years', '1 years', '2 years', '3 years', '4 years', '5 years', '6 years']
    colors = ['dodgerblue', 'whitesmoke', 'whitesmoke', 'whitesmoke', 'whitesmoke', 'whitesmoke', 'whitesmoke']
    explode = (0.1, 0, 0, 0, 0, 0, 0) 
    
    plt.pie(df.tenure_years.value_counts(), explode=explode, colors=colors, labels = labels, autopct='%1.1f%%', shadow=True, textprops={'fontsize':14}, wedgeprops={'edgecolor': 'black', 'width': 0.6})
    plt.title('Percent of Accounts by Tenure Years', fontsize=18)
    plt.show()


def correlation_exploration(df, x_string, y_string):
    '''
    This nifty function takes in a df, a string for x variable,
    and a string for y variable and displays their correlation.
    '''
    r, p = stats.pearsonr(df[x_string], df[y_string])
    df.plot.scatter(x_string, y_string)
    plt.title(f"{x_string}'s Relationship with {y_string}")
    print(f'The p-value is: {p}. There is {round(p,3)}% chance that we see these results by chance.')
    print(f'r = {round(r, 2)}')
    plt.show()


def tax_distribution_viz(df):
    '''
    This function takes in my Zillow df and plots the distribution
    of the tax rate for Los Angeles, Orange, and Ventura counties.
    '''
    los_angeles_tax_dist = df[df.county_name == "Los Angeles"].tax_rate
    orange_tax_dist = df[df.county_name == "Orange"].tax_rate
    ventura_tax_dist = df[df.county_name == "Ventura"].tax_rate

    plt.figure(figsize=(16,14))

    plt.subplot(3,1,1)
    sns.distplot(los_angeles_tax_dist, bins=50, kde=True, rug=True)
    plt.xlim(0, .10)
    plt.ylim(0, 600)
    plt.title("Los Angeles County Tax Distribution")

    plt.subplot(3,1,2)
    sns.distplot(orange_tax_dist, bins=50, kde=True, rug=True, color='orange')
    plt.xlim(0, .10)
    plt.ylim(0, 600)
    plt.title("Orange County Tax Distribution")

    plt.subplot(3,1,3)
    sns.distplot(ventura_tax_dist, bins=50, kde=True, rug=True, color='green')
    plt.xlim(0, .10)
    plt.ylim(0, 600)
    plt.title("Ventura County Tax Distribution")

    plt.tight_layout()

    plt.show()


def anatomy_of_a_figure():
    np.random.seed(19680801)

    X = np.linspace(0.5, 3.5, 100)
    Y1 = 3+np.cos(X)
    Y2 = 1+np.cos(1+X/0.75)/2
    Y3 = np.random.uniform(Y1, Y2, len(X))

    fig = plt.figure(figsize=(8, 8))
    ax = fig.add_subplot(1, 1, 1, aspect=1)


    def minor_tick(x, pos):
        if not x % 1.0:
            return ""
        return "%.2f" % x

    ax.xaxis.set_major_locator(MultipleLocator(1.000))
    ax.xaxis.set_minor_locator(AutoMinorLocator(4))
    ax.yaxis.set_major_locator(MultipleLocator(1.000))
    ax.yaxis.set_minor_locator(AutoMinorLocator(4))
    ax.xaxis.set_minor_formatter(FuncFormatter(minor_tick))

    ax.set_xlim(0, 4)
    ax.set_ylim(0, 4)

    ax.tick_params(which='major', width=1.0)
    ax.tick_params(which='major', length=10)
    ax.tick_params(which='minor', width=1.0, labelsize=10)
    ax.tick_params(which='minor', length=5, labelsize=10, labelcolor='0.25')

    ax.grid(linestyle="--", linewidth=0.5, color='.25', zorder=-10)

    ax.plot(X, Y1, c=(0.25, 0.25, 1.00), lw=2, label="Blue signal", zorder=10)
    ax.plot(X, Y2, c=(1.00, 0.25, 0.25), lw=2, label="Red signal")
    ax.plot(X, Y3, linewidth=0,
            marker='o', markerfacecolor='w', markeredgecolor='k')

    ax.set_title("Anatomy of a figure", fontsize=20, verticalalignment='bottom')
    ax.set_xlabel("X axis label")
    ax.set_ylabel("Y axis label")

    ax.legend()


    def circle(x, y, radius=0.15):
        from matplotlib.patches import Circle
        from matplotlib.patheffects import withStroke
        circle = Circle((x, y), radius, clip_on=False, zorder=10, linewidth=1,
                        edgecolor='black', facecolor=(0, 0, 0, .0125),
                        path_effects=[withStroke(linewidth=5, foreground='w')])
        ax.add_artist(circle)


    def text(x, y, text):
        ax.text(x, y, text, backgroundcolor="white",
                ha='center', va='top', weight='bold', color='blue')


    # Minor tick
    circle(0.50, -0.10)
    text(0.50, -0.32, "Minor tick label")

    # Major tick
    circle(-0.03, 4.00)
    text(0.03, 3.80, "Major tick")

    # Minor tick
    circle(0.00, 3.50)
    text(0.00, 3.30, "Minor tick")

    # Major tick label
    circle(-0.15, 3.00)
    text(-0.15, 2.80, "Major tick label")

    # X Label
    circle(1.80, -0.27)
    text(1.80, -0.45, "X axis label")

    # Y Label
    circle(-0.27, 1.80)
    text(-0.27, 1.6, "Y axis label")

    # Title
    circle(1.60, 4.13)
    text(1.60, 3.93, "Title")

    # Blue plot
    circle(1.75, 2.80)
    text(1.75, 2.60, "Line\n(line plot)")

    # Red plot
    circle(1.20, 0.60)
    text(1.20, 0.40, "Line\n(line plot)")

    # Scatter plot
    circle(3.20, 1.75)
    text(3.20, 1.55, "Markers\n(scatter plot)")

    # Grid
    circle(3.00, 3.00)
    text(3.00, 2.80, "Grid")

    # Legend
    circle(3.70, 3.80)
    text(3.70, 3.60, "Legend")

    # Axes
    circle(0.5, 0.5)
    text(0.5, 0.3, "Axes")

    # Figure
    circle(-0.3, 0.65)
    text(-0.3, 0.45, "Figure")

    color = 'blue'
    ax.annotate('Spines', xy=(4.0, 0.35), xytext=(3.3, 0.5),
                weight='bold', color=color,
                arrowprops=dict(arrowstyle='->',
                                connectionstyle="arc3",
                                color=color))

    ax.annotate('', xy=(3.15, 0.0), xytext=(3.45, 0.45),
                weight='bold', color=color,
                arrowprops=dict(arrowstyle='->',
                                connectionstyle="arc3",
                                color=color))

    ax.text(4.0, -0.4, "Made with http://matplotlib.org",
            fontsize=10, ha="right", color='.5')

    plt.show()